/*!
 * Copyright 2019 Google LLC. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import * as assert from 'assert';
import * as grpc from 'grpc';
import {Database, Instance, SessionPool, Snapshot, Spanner} from '../src';
import * as mock from './mockserver/mockspanner';
import * as mockInstanceAdmin from './mockserver/mockinstanceadmin';
import * as mockDatabaseAdmin from './mockserver/mockdatabaseadmin';
import * as sinon from 'sinon';
import {google} from '../protos/protos';
import {types} from '../src/session';
import {ExecuteSqlRequest} from '../src/transaction';
import {Row} from '../src/partial-result-stream';
import {
  SessionLeakError,
  SessionPoolExhaustedError,
  SessionPoolOptions,
} from '../src/session-pool';
import {ServiceError} from 'grpc';

function numberToEnglishWord(num: number): string {
  switch (num) {
    case 1:
      return 'One';
    case 2:
      return 'Two';
    case 3:
      return 'Three';
    default:
      throw new Error(`Unknown or unsupported number: ${num}`);
  }
}

describe('Spanner with mock server', () => {
  let sandbox: sinon.SinonSandbox;
  const selectSql = 'SELECT NUM, NAME FROM NUMBERS';
  const invalidSql = 'SELECT * FROM FOO';
  const insertSql = `INSERT INTO NUMBER (NUM, NAME) VALUES (4, 'Four')`;
  const fooNotFoundErr = Object.assign(new Error('Table FOO not found'), {
    code: grpc.status.NOT_FOUND,
  });
  const server = new grpc.Server();
  const spannerMock = mock.createMockSpanner(server);
  mockInstanceAdmin.createMockInstanceAdmin(server);
  mockDatabaseAdmin.createMockDatabaseAdmin(server);
  let spanner: Spanner;
  let instance: Instance;
  let dbCounter = 1;

  function newTestDatabase(options?: SessionPoolOptions): Database {
    return instance.database(`database-${dbCounter++}`, options);
  }

  before(() => {
    sandbox = sinon.createSandbox();
    const port = server.bind(
      '0.0.0.0:0',
      grpc.ServerCredentials.createInsecure()
    );
    server.start();
    spannerMock.putStatementResult(
      selectSql,
      mock.StatementResult.resultSet(mock.createSimpleResultSet())
    );
    spannerMock.putStatementResult(
      invalidSql,
      mock.StatementResult.error(fooNotFoundErr)
    );
    spannerMock.putStatementResult(
      insertSql,
      mock.StatementResult.updateCount(1)
    );

    // TODO(loite): Enable when SPANNER_EMULATOR_HOST is supported.
    // Set environment variable for SPANNER_EMULATOR_HOST to the mock server.
    // process.env.SPANNER_EMULATOR_HOST = `localhost:${port}`;
    spanner = new Spanner({
      projectId: 'fake-project-id',
      servicePath: 'localhost',
      port,
      sslCreds: grpc.credentials.createInsecure(),
    });
    // Gets a reference to a Cloud Spanner instance and database
    instance = spanner.instance('instance');
  });

  after(() => {
    server.tryShutdown(() => {});
    delete process.env.SPANNER_EMULATOR_HOST;
    sandbox.restore();
  });

  it('should execute query', async () => {
    // The query to execute
    const query = {
      sql: selectSql,
    };
    const database = newTestDatabase();
    try {
      const [rows] = await database.run(query);
      assert.strictEqual(rows.length, 3);
      let i = 0;
      rows.forEach(row => {
        i++;
        assert.strictEqual(row[0].name, 'NUM');
        assert.strictEqual(row[0].value.valueOf(), i);
        assert.strictEqual(row[1].name, 'NAME');
        assert.strictEqual(row[1].value.valueOf(), numberToEnglishWord(i));
      });
    } finally {
      await database.close();
    }
  });

  it('should execute update', async () => {
    const update = {
      sql: insertSql,
    };
    const database = newTestDatabase();
    try {
      const updated = await executeSimpleUpdate(database, update);
      assert.deepStrictEqual(updated, [1]);
    } finally {
      await database.close();
    }
  });

  it('should throw an error with a stacktrace when leaking a session', async () => {
    await testLeakSession();
  });

  async function testLeakSession() {
    // The query to execute
    const query = {
      sql: selectSql,
    };
    const db = newTestDatabase();
    let transaction: Snapshot;
    await db
      .getSnapshot({strong: true, returnReadTimestamp: true})
      .then(([tx]) => {
        transaction = tx;
        return tx.run(query);
      })
      .then(([rows]) => {
        // Assert that we get all results from the server.
        assert.strictEqual(rows.length, 3);
        // Note that we do not call transaction.end(). This will cause a session leak.
      })
      .catch(reason => {
        assert.fail(reason);
      });
    await db
      .close()
      .then(() => {
        assert.fail('Missing expected SessionLeakError');
      })
      .catch((reason: SessionLeakError) => {
        assert.strictEqual(reason.name, 'SessionLeakError', reason);
        assert.strictEqual(reason.messages.length, 1);
        assert.ok(reason.messages[0].indexOf('testLeakSession') > -1);
      });
  }

  it('should reuse sessions', async () => {
    const database = newTestDatabase();
    try {
      await verifyReadSessionReuse(database);
    } finally {
      await database.close();
    }
  });

  it('should reuse sessions when fail=true', async () => {
    const db = newTestDatabase({
      max: 10,
      concurrency: 5,
      writes: 0.1,
      fail: true,
    });
    try {
      await verifyReadSessionReuse(db);
    } finally {
      await db.close();
    }
  });

  async function verifyReadSessionReuse(database: Database) {
    // The query to execute
    const query = {
      sql: selectSql,
    };
    const pool = database.pool_ as SessionPool;
    let sessionId = '';
    for (let i = 0; i < 10; i++) {
      const [rows] = await database.run(query);
      assert.strictEqual(rows.length, 3);
      rows.forEach(() => {});
      assert.strictEqual(pool.size, 1);
      if (i > 0) {
        assert.strictEqual(pool._inventory[types.ReadOnly][0].id, sessionId);
      }
      sessionId = pool._inventory[types.ReadOnly][0].id;
    }
  }

  it('should throw SessionPoolExhaustedError with stacktraces when pool is exhausted', async () => {
    await testSessionPoolExhaustedError();
  });

  async function testSessionPoolExhaustedError() {
    // The query to execute
    const query = {
      sql: selectSql,
    };
    const database = newTestDatabase({
      max: 1,
      fail: true,
    });
    try {
      const [tx1] = await database.getSnapshot();
      try {
        const [tx2] = await database.getSnapshot();
        assert.fail('missing expected exception');
      } catch (e) {
        assert.strictEqual(e.name, SessionPoolExhaustedError.name);
        const exhausted = e as SessionPoolExhaustedError;
        assert.ok(exhausted.messages);
        assert.strictEqual(exhausted.messages.length, 1);
        assert.ok(
          exhausted.messages[0].indexOf('testSessionPoolExhaustedError') > -1
        );
      }
      tx1.end();
    } finally {
      await database.close();
    }
  }

  it('should reuse sessions after executing streaming sql', async () => {
    // The query to execute
    const query = {
      sql: selectSql,
    };
    const database = newTestDatabase();
    try {
      const pool = database.pool_ as SessionPool;
      for (let i = 0; i < 10; i++) {
        const rowCount = await getRowCountFromStreamingSql(database, query);
        assert.strictEqual(rowCount, 3);
      }
      assert.strictEqual(pool.size, 1);
    } finally {
      await database.close();
    }
  });

  it('should reuse sessions after executing an invalid streaming sql', async () => {
    // The query to execute
    const query = {
      sql: invalidSql,
    };
    const database = newTestDatabase();
    try {
      const pool = database.pool_ as SessionPool;
      for (let i = 0; i < 10; i++) {
        try {
          const rowCount = await getRowCountFromStreamingSql(database, query);
          assert.fail('missing expected exception');
        } catch (e) {
          assert.strictEqual(
            e.message,
            `${grpc.status.NOT_FOUND} NOT_FOUND: ${fooNotFoundErr.message}`
          );
        }
      }
      assert.strictEqual(pool.size, 1);
    } finally {
      await database.close();
    }
  });

  function getRowCountFromStreamingSql(
    database: Database,
    query: ExecuteSqlRequest
  ): Promise<number> {
    return new Promise<number>((resolve, reject) => {
      let rows = 0;
      database
        .runStream(query)
        .on('error', err => {
          reject(err);
        })
        .on('data', row => {
          rows++;
        })
        .on('end', res => {
          return resolve(rows);
        });
    });
  }

  it('should reuse write sessions', async () => {
    const database = newTestDatabase();
    try {
      await verifyWriteSessionReuse(database);
    } finally {
      await database.close();
    }
  });

  it('should reuse write sessions when fail=true', async () => {
    const db = newTestDatabase({
      max: 10,
      concurrency: 5,
      writes: 0.1,
      fail: true,
    });
    try {
      await verifyWriteSessionReuse(db);
    } finally {
      await db.close();
    }
  });

  async function verifyWriteSessionReuse(database: Database) {
    const update = {
      sql: insertSql,
    };
    const pool = database.pool_ as SessionPool;
    for (let i = 0; i < 10; i++) {
      const updated = await executeSimpleUpdate(database, update);
      // The pool should not contain more sessions than the number of transactions that we have executed.
      // The exact number depends on the time needed to prepare new transactions, as checking in a read/write
      // transaction to the pool will cause the session to be prepared with a read/write transaction before it is added
      // to the list of available sessions.
      assert.ok(pool.size <= i + 1);
    }
  }

  it('should execute queries in parallel', async () => {
    // The query to execute
    const query = {
      sql: selectSql,
    };
    const database = newTestDatabase();
    try {
      const pool = database.pool_ as SessionPool;
      const promises: Array<Promise<Row[]>> = [];
      for (let i = 0; i < 10; i++) {
        promises.push(database.run(query));
      }
      await Promise.all(promises);
      assert.ok(
        pool.size >= 1 && pool.size <= 10,
        'Pool size should be between 1 and 10'
      );
    } finally {
      await database.close();
    }
  });

  it('should execute updates in parallel', async () => {
    spannerMock.freeze();
    const update = {
      sql: insertSql,
    };
    const database = newTestDatabase();
    try {
      const pool = database.pool_ as SessionPool;
      const promises: Array<Promise<number | number[]>> = [];
      for (let i = 0; i < 10; i++) {
        promises.push(executeSimpleUpdate(database, update));
      }
      spannerMock.unfreeze();
      await Promise.all(promises);
      assert.strictEqual(pool.size, 10);
    } finally {
      await database.close();
    }
  });

  it('should fail on session pool exhaustion and fail=true', async () => {
    // The query to execute
    const query = {
      sql: selectSql,
    };
    const database = newTestDatabase({
      max: 1,
      fail: true,
    });
    let tx1;
    try {
      try {
        [tx1] = await database.getSnapshot();
        await database.getSnapshot();
        assert.fail('missing expected exception');
      } catch (e) {
        assert.strictEqual(e.message, 'No resources available.');
      }
    } finally {
      if (tx1) {
        tx1.end();
      }
      await database.close();
    }
  });

  it('should return different database instances when the same database is requested twice with different session pool options', async () => {
    const dbWithDefaultOptions = newTestDatabase();
    const dbWithWriteSessions = instance.database(dbWithDefaultOptions.id!, {
      writes: 1.0,
    });
    assert.notStrictEqual(dbWithDefaultOptions, dbWithWriteSessions);
  });

  it('should list instance configurations', async () => {
    const [configs] = await spanner.getInstanceConfigs();
    assert.strictEqual(configs.length, 1);
  });

  it('should list instances', async () => {
    const [instances] = await spanner.getInstances();
    assert.strictEqual(instances.length, 2);
  });

  it('should create an instance', async () => {
    const [createdInstance] = await spanner
      .createInstance('new-instance', {
        config: 'test-instance-config',
        nodes: 10,
      })
      .then(data => {
        const operation = data[1];
        return operation.promise();
      })
      .then(instance => {
        return instance;
      });
    assert.strictEqual(
      createdInstance.name,
      `projects/${spanner.projectId}/instances/new-instance`
    );
    assert.strictEqual(createdInstance.nodeCount, 10);
  });

  it('should update an instance', async () => {
    const instance = spanner.instance(mockInstanceAdmin.PROD_INSTANCE_NAME);
    const [updatedInstance] = await instance
      .setMetadata({
        nodeCount: 20,
        displayName: 'Production instance with 20 nodes',
      })
      .then(data => {
        return data[0].promise() as Promise<
          [google.spanner.admin.instance.v1.Instance]
        >;
      })
      .then(instance => {
        return instance;
      });
    assert.strictEqual(updatedInstance.nodeCount, 20);
  });

  it('should delete an instance', async () => {
    const instance = spanner.instance(mockInstanceAdmin.PROD_INSTANCE_NAME);
    const [res] = await instance.delete();
    assert.ok(res);
  });

  it('should list databases', async () => {
    const [databases] = await instance.getDatabases();
    assert.strictEqual(databases.length, 2);
  });

  it('should create a database', async () => {
    const [createdDatabase] = await instance
      .createDatabase('new-database')
      .then(data => {
        const operation = data[1];
        return operation.promise();
      })
      .then(database => {
        return database as [google.spanner.admin.database.v1.Database];
      });
    assert.strictEqual(
      createdDatabase.name,
      `${instance.formattedName_}/databases/new-database`
    );
  });
});

function executeSimpleUpdate(
  database: Database,
  update: string | ExecuteSqlRequest
): Promise<number | [number]> {
  return database
    .runTransactionAsync<[number]>(
      (transaction): Promise<[number]> => {
        return transaction
          .runUpdate(update)
          .then(rowCount => {
            return rowCount;
          })
          .then(rowCount => {
            return transaction.commit().then(() => rowCount);
          })
          .then(rowCount => {
            return rowCount;
          })
          .catch(() => {
            transaction.rollback().then(() => {});
            return [-1];
          });
      }
    )
    .then(updated => {
      return updated;
    });
}
