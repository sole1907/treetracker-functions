
import psycopg2



def transfer(dest_conn,src_conn, organization_id, action = False):
    """
    Transfer organizations data and related planter, trees from source to target.
 
    Args:
        target (string): The target database URL.
        source (string): The source database URL.
        org_name (string): The name of the desired organization
        org_id (int): The id of the desired organization
        action(boolean):Whether to update the database when the inserted row already exists.
 
    Returns:
        None.
    """

    #connect source database
    src_cur = src_conn.cursor()
    org_name = organization_id


    dest_cur = dest_conn.cursor()

    
    def insert_or_update(table_name, columns, data, dest_cur, dest_conn, action=False, conflict_column='id'):
        """Inserts or updates records in the specified table.

            Args:
                table_name (string): The target table name.
                column(string list): The column names of target table.
                data (string list): The data to insert
                action(boolean): Update OR do nothing when there is duplicate
        
            Returns:
                None.
        
        """
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        update_statements = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns])

        if action:
            #if action == True, update the database if row already exists

            query = f"""
                INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})
                ON CONFLICT ({conflict_column}) DO UPDATE SET {update_statements};
            """
        else:
            query = f"""
                INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})
                ON CONFLICT ({conflict_column}) DO NOTHING;
            """
        dest_cur.executemany(query, data)
        dest_conn.commit()

    #fetch and update organization with id
    src_cur.execute("SELECT * FROM organizations WHERE id = %s;", (org_name,))
    organizations = src_cur.fetchone()
    org_id = organizations[0]

    if organizations:
        org_columns = [desc[0] for desc in src_cur.description]
        insert_or_update("organizations", org_columns, [organizations], dest_cur, dest_conn, action=action)
    else:
        print("No such organization")

    #update entity?
    #fetch planter with given organizaiton id and insert/update 
    src_cur.execute("SELECT * FROM planter WHERE organization_id = %s", (org_id,))
    planters_data = src_cur.fetchall()
    planter_columns = [desc[0] for desc in src_cur.description]
    insert_or_update("planter", planter_columns, planters_data, dest_cur, dest_conn, action=action)

    ### update tree and species with join operation -- seems slower.
    # query = """
    # SELECT trees.*, specie.*
    # FROM planter
    # JOIN trees ON planter.id = trees.planter_id
    # LEFT JOIN tree_species ON trees.species_id = tree_species.id
    # WHERE planter.organization_id = %s;
    # """
    # src_cur.execute(query, (org_id,))
    # result_data = src_cur.fetchall()

    # # Insert or update trees in the target database
    # tree_columns = [desc[0] for i, desc in enumerate(src_cur.description) if i < 47]
    # for row in result_data:
    #     tree_data = row[:47]
    #     insert_or_update("trees", tree_columns, [tree_data],  dest_cur, dest_conn, action=action)

    # # Insert species in the target database if not exists
    # species_columns = [desc[0] for i, desc in enumerate(src_cur.description) if i >= 47]
    # for row in result_data:
    #     species_data = row[47:]
    #     if species_data and species_data[0] is not None:  # Check if species data exists for the tree
    #         insert_or_update("tree_species", species_columns, [species_data],  dest_cur, dest_conn, action=action)

    ## update without join operation ---- compare efficiency with join operation
    # update trees assoicated with each planter
    for planter in planters_data:
        planter_id = planter[0]  
        # Fetch trees associated with the current planter
        src_cur.execute("SELECT * FROM trees WHERE planter_id = %s", (planter_id,))
        trees_data = src_cur.fetchall()
        tree_columns = [desc[0] for desc in src_cur.description]
        insert_or_update("trees", tree_columns, trees_data, dest_cur, dest_conn, action=action)

        for tree in trees_data:
            species_id = tree[29] #index of species_id = 29

            src_cur.execute("SELECT * FROM tree_species WHERE id = %s", (species_id,))
            species_data = src_cur.fetchone()
            
            if species_data:
                species_columns = [desc[0] for desc in src_cur.description]
                insert_or_update("tree_species", species_columns, [species_data],  dest_cur, dest_conn, action=action)

    src_cur.close()
    dest_cur.close()
    src_conn.close()
    dest_conn.close()

    return 