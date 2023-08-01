import { handle } from '../index.js';
import dbPoolConnection from "../connection-pool.js";

jest.mock('../connection-pool.js', () => {
  return {
    query: jest.fn().mockImplementation(sqlQuery => {
      if (sqlQuery.includes('WHERE public.denormalized_trees.id = trees_fetch.id'))
        return Promise.resolve({ rowCount: 10 });
      return Promise.resolve({ rowCount: 1 });
    }),
  };
});

describe('unit testing', () => {
  let context;

  beforeEach(() => {
    context = {
      method: 'GET',
      query: {},
      log: console,
    };
  });

  it('should run without errors for GET requests without tree-id', async () => {
    await expect(handle(context, {})).resolves.not.toThrow();
    expect(dbPoolConnection.query).toHaveBeenCalled();
  });

  it('should return 405 for non-GET requests', async () => {
    context.method = 'POST';
    const result = await handle(context, {});
    expect(result).toEqual({ statusCode: 405, statusMessage: 'Method not allowed' });
  });

  it('should return 405 for non-number tree-id', async () => {
    context.query['tree-id'] = 'invalid';
    const result = await handle(context, {});
    expect(result).toEqual({ statusCode: 405, statusMessage: 'tree-id must be a number' });
  });

  it('should update only one row when we set treeid', async () => {
    context.query['tree-id'] = '1';
    const result = await handle(context, {});
    console.log(result)
    expect(result).toEqual({ statusCode: 200, rowsUpdated: 1 });
  });

  it('should update all rows', async () => {
    const result = await handle(context, {});
    console.log(result)
    expect(result).toEqual({ statusCode: 200, rowsUpdated: 10 });
  });

  // Add more tests 
});
