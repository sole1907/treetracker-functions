import dbPoolConnection from "./connection-pool.js";
/**
 * Your HTTP handling function, invoked with each request. This is an example
 * function that echoes its input to the caller, and returns an error if
 * the incoming request is something other than an HTTP POST or GET.
 *
 * In can be invoked with 'func invoke'
 * It can be tested with 'npm test'
 *
 * @param {Context} context a context object.
 * @param {object} context.body the request body if any
 * @param {object} context.query the query string deserialized as an object, if any
 * @param {object} context.log logging object with methods for 'info', 'warn', 'error', etc.
 * @param {object} context.headers the HTTP request headers
 * @param {string} context.method the HTTP request method
 * @param {string} context.httpVersion the HTTP protocol version
 * See: https://github.com/knative/func/blob/main/docs/function-developers/nodejs.md#the-context-object
 */

function isValidTreeId(treeId) {
  return !isNaN(+treeId);
}

let dbConnection = null;

const handle = async (context, body) => {
  // If the request is an HTTP POST, the context will contain the request body
  if (context.method !== 'GET')
    return { statusCode: 405, statusMessage: 'Method not allowed' };
  const treeId = context.query['tree-id']

  if(treeId && !isValidTreeId(treeId))
    return { statusCode: 405, statusMessage: 'tree-id must be a number' };
  
  // If the request is an HTTP GET, the context will include a query string, if it exists

  try {
    
    if(!dbConnection) dbConnection = await dbPoolConnection;
    const startTime = process.hrtime();
    let sqlQueryToUpdateEitherOneTreeOrALlTrees = `
      UPDATE public.denormalized_trees
        SET 
          estimated_geometric_point = trees_fetch.estimated_geometric_location,
          planter_id = trees_fetch.planter_id,
          planter_name = plt.first_name,
          species_id = spc.uuid,
          species_name = spc.name,
          token_id = tkn.id,
          wallet_id = wlt.id,
          wallet_name = wlt.name,
          tags = (       
                SELECT jsonb_agg(tag)
            FROM public.tree_tag tree_tag
            JOIN public.tag tag ON tag.id = tree_tag.tag_id
            WHERE tree_tag.tree_id = trees_fetch.id
          ),
          organizations = get_organization_list_by_planter_id(plt.id),
          country_id = country.id,
          continent_id = continent.id
        from public.trees trees_fetch
          LEFT JOIN public.planter AS plt ON plt.id = trees_fetch.planter_id
          LEFT JOIN public.tree_species spc ON spc.id = trees_fetch.species_id
          LEFT JOIN wallet.token AS tkn ON tkn.capture_id::text = trees_fetch.uuid::text
          LEFT JOIN wallet.wallet AS wlt ON tkn.wallet_id  = wlt.id
          JOIN region country ON st_within(trees_fetch.estimated_geometric_location, country.geom) 
            AND (country.type_id IN ( 
              SELECT region_type.id FROM region_type WHERE region_type.type::text = 'country'::text)
              )
          JOIN region continent ON st_within(trees_fetch.estimated_geometric_location, continent.geom) 
            AND (continent.type_id IN ( 
              SELECT region_type.id  FROM region_type WHERE region_type.type::text = 'continents'::text)
              )
      `;

    if (treeId) {
      sqlQueryToUpdateEitherOneTreeOrALlTrees += `
            WHERE trees_fetch.id = ${+treeId} and public.denormalized_trees.id = ${+treeId}
          `;
    } else {
      sqlQueryToUpdateEitherOneTreeOrALlTrees += `
          WHERE public.denormalized_trees.id = trees_fetch.id
        `;
    }
    const results = await dbConnection.query(sqlQueryToUpdateEitherOneTreeOrALlTrees);
    const endTime = process.hrtime(startTime);
    console.log(`Process successfully finished by ${endTime[0]} seconds, ${results.rowCount} recorded got updated`)
    return { statusCode: 200, rowsUpdated: results.rowCount };
  } catch (error) {
    console.log('something went wrong while updating denormalized tree table', error)
  }
}

// Export the function
export { handle };
