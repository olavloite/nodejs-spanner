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

import {google} from '../../protos/protos';
import * as grpc from 'grpc';
import * as protoLoader from '@grpc/proto-loader';
import protobuf = google.spanner.v1;
import Timestamp = google.protobuf.Timestamp;

const PROTO_PATH = 'spanner.proto';
const IMPORT_PATH = __dirname + '/../../../protos';
const PROTO_DIR = __dirname + '/../../../protos/google/spanner/v1';

/**
 * Load the Spanner service proto.
 */
const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
  keepCase: false,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
  includeDirs: [IMPORT_PATH, PROTO_DIR],
});
const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
const spannerProtoDescriptor = protoDescriptor['google']['spanner']['v1'];

/**
 * The type of result for an SQL statement that the mock server should return.
 */
enum StatementResultType {
  ERROR,
  RESULT_SET,
  UPDATE_COUNT,
}

/**
 * StatementResult contains the result for an SQL statement on the mock server.
 */
export class StatementResult {
  private readonly _type: StatementResultType;
  get type(): StatementResultType {
    return this._type;
  }
  private readonly _error: Error | null;
  get error(): Error {
    if (this._error) {
      return this._error;
    }
    throw new Error('The StatementResult does not contain an Error');
  }
  private readonly _resultSet: protobuf.ResultSet | null;
  get resultSet(): protobuf.ResultSet {
    if (this._resultSet) {
      return this._resultSet;
    }
    throw new Error('The StatementResult does not contain a ResultSet');
  }
  private readonly _updateCount: number | null;
  get updateCount(): number {
    if (this._updateCount) {
      return this._updateCount;
    }
    throw new Error('The StatementResult does not contain an UpdateCount');
  }

  private constructor(
    type: StatementResultType,
    error: Error | null,
    resultSet: protobuf.ResultSet | null,
    updateCount: number | null
  ) {
    this._type = type;
    this._error = error;
    this._resultSet = resultSet;
    this._updateCount = updateCount;
  }

  /**
   * Create a StatementResult that will return an error.
   * @param error The error to return for the statement.
   */
  static error(error: Error): StatementResult {
    return new StatementResult(StatementResultType.ERROR, error, null, null);
  }

  /**
   * Create a StatementResult that will return a ResultSet or a stream of PartialResultSets.
   * @param resultSet The result set to return.
   */
  static resultSet(resultSet: protobuf.ResultSet): StatementResult {
    return new StatementResult(
      StatementResultType.RESULT_SET,
      null,
      resultSet,
      null
    );
  }

  /**
   * Create a StatementResult that will return an update count.
   * @param updateCount The row count to return.
   */
  static updateCount(updateCount: number): StatementResult {
    return new StatementResult(
      StatementResultType.UPDATE_COUNT,
      null,
      null,
      updateCount
    );
  }
}

export function createUnimplementedError(msg: string): grpc.ServiceError {
  const error = new Error(msg);
  return Object.assign(error, {
    code: grpc.status.UNIMPLEMENTED,
  });
}

/**
 * MockSpanner is a mocked in-mem Spanner server that manages sessions and transactions automatically. Results for SQL statements must be registered on the server using the MockSpanner.putStatementResult function.
 */
export class MockSpanner {
  private frozen = 0;
  private sessionCounter = 0;
  private sessions: Map<string, protobuf.Session> = new Map<
    string,
    protobuf.Session
  >();
  private transactionCounters: Map<string, number> = new Map<string, number>();
  private transactions: Map<string, protobuf.Transaction> = new Map<
    string,
    protobuf.Transaction
  >();
  private transactionOptions: Map<
    string,
    protobuf.ITransactionOptions | null | undefined
  > = new Map<string, protobuf.ITransactionOptions | null | undefined>();
  private statementResults: Map<string, StatementResult> = new Map<
    string,
    StatementResult
  >();

  private constructor() {
    this.putStatementResult = this.putStatementResult.bind(this);
    this.batchCreateSessions = this.batchCreateSessions.bind(this);
    this.createSession = this.createSession.bind(this);
    this.deleteSession = this.deleteSession.bind(this);
    this.getSession = this.getSession.bind(this);

    this.beginTransaction = this.beginTransaction.bind(this);
    this.commit = this.commit.bind(this);
    this.rollback = this.rollback.bind(this);

    this.executeStreamingSql = this.executeStreamingSql.bind(this);
  }

  /**
   * Creates a MockSpanner instance.
   */
  static create(): MockSpanner {
    return new MockSpanner();
  }

  /**
   * Registers a result for an SQL statement on the server.
   * @param sql The SQL statement that should return the result.
   * @param result The result to return.
   */
  putStatementResult(sql: string, result: StatementResult) {
    this.statementResults.set(sql, result);
  }

  freeze() {
    this.frozen++;
  }

  unfreeze() {
    if (this.frozen === 0) {
      throw new Error('This mock server is already unfrozen');
    }
    this.frozen--;
  }

  /**
   * Creates a new session for the given database and adds it to the map of sessions of this server.
   * @param database The database to create the session for.
   */
  private newSession(database: string): protobuf.Session {
    const id = this.sessionCounter++;
    const name = `${database}/sessions/${id}`;
    const session = protobuf.Session.create({name, createTime: now()});
    this.sessions.set(name, session);
    return session;
  }

  private static createSessionNotFoundError(name: string): grpc.ServiceError {
    const error = new Error(`Session not found: ${name}`);
    return Object.assign(error, {
      code: grpc.status.NOT_FOUND,
    });
  }

  private static createTransactionNotFoundError(
    name: string
  ): grpc.ServiceError {
    const error = new Error(`Transaction not found: ${name}`);
    return Object.assign(error, {
      code: grpc.status.NOT_FOUND,
    });
  }

  private static sleep(ms): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  private async simulateExecutionTime(): Promise<void> {
    while (this.frozen > 0) {
      await MockSpanner.sleep(10);
    }
  }

  batchCreateSessions(
    call: grpc.ServerUnaryCall<protobuf.BatchCreateSessionsRequest>,
    callback: protobuf.Spanner.BatchCreateSessionsCallback
  ) {
    this.simulateExecutionTime().then(() => {
      const sessions = new Array<protobuf.Session>();
      for (let i = 0; i < call.request.sessionCount; i++) {
        sessions.push(this.newSession(call.request.database));
      }
      callback(
        null,
        protobuf.BatchCreateSessionsResponse.create({session: sessions})
      );
    });
  }

  createSession(
    call: grpc.ServerUnaryCall<protobuf.CreateSessionRequest>,
    callback: protobuf.Spanner.CreateSessionCallback
  ) {
    this.simulateExecutionTime().then(() => {
      callback(null, this.newSession(call.request.database));
    });
  }

  getSession(
    call: grpc.ServerUnaryCall<protobuf.GetSessionRequest>,
    callback: protobuf.Spanner.GetSessionCallback
  ) {
    this.simulateExecutionTime().then(() => {
      const session = this.sessions[call.request.name];
      if (session) {
        callback(null, session);
      } else {
        callback(MockSpanner.createSessionNotFoundError(call.request.name));
      }
    });
  }

  listSessions(
    call: grpc.ServerUnaryCall<protobuf.ListSessionsRequest>,
    callback: protobuf.Spanner.ListSessionsCallback
  ) {
    this.simulateExecutionTime().then(() => {
      callback(
        null,
        protobuf.ListSessionsResponse.create({
          sessions: Array.from(this.sessions.values()),
        })
      );
    });
  }

  deleteSession(
    call: grpc.ServerUnaryCall<protobuf.DeleteSessionRequest>,
    callback: protobuf.Spanner.DeleteSessionCallback
  ) {
    if (this.sessions.delete(call.request.name)) {
      callback(null, google.protobuf.Empty.create());
    } else {
      callback(MockSpanner.createSessionNotFoundError(call.request.name));
    }
  }

  executeSql(
    call: grpc.ServerUnaryCall<protobuf.ExecuteSqlRequest>,
    callback: protobuf.Spanner.ExecuteSqlCallback
  ) {
    callback(createUnimplementedError('ExecuteSql is not yet implemented'));
  }

  executeStreamingSql(
    call: grpc.ServerWritableStream<protobuf.ExecuteSqlRequest>
  ) {
    const res = this.statementResults.get(call.request.sql);
    if (res) {
      switch (res.type) {
        case StatementResultType.RESULT_SET:
          const partialResultSets = MockSpanner.toPartialResultSets(
            res.resultSet
          );
          for (const partial of partialResultSets) {
            call.write(partial);
          }
          break;
        case StatementResultType.UPDATE_COUNT:
          call.write(MockSpanner.toPartialResultSet(res.updateCount));
          break;
        case StatementResultType.ERROR:
          call.write(res.error);
          break;
        default:
          call.write(new Error(`Unknown StatementResult type: ${res.type}`));
      }
      call.end();
    } else {
      call.destroy(
        new Error(`There is no result registered for ${call.request.sql}`)
      );
    }
  }

  /**
   * Splits a ResultSet into one PartialResultSet per row. This ensure that we can also test returning multiple partial results sets from a streaming method.
   * @param resultSet The ResultSet to split.
   */
  private static toPartialResultSets(
    resultSet: protobuf.ResultSet
  ): protobuf.PartialResultSet[] {
    const res: protobuf.PartialResultSet[] = [];
    let first = true;
    for (let i = 0; i < resultSet.rows.length; i++) {
      const token = i.toString().padStart(8, '0');
      const partial = protobuf.PartialResultSet.create({
        resumeToken: Buffer.from(token),
        values: resultSet.rows[i].values,
      });
      if (first) {
        partial.metadata = resultSet.metadata;
        first = false;
      }
      res.push(partial);
    }
    return res;
  }

  private static toPartialResultSet(
    rowCount: number
  ): protobuf.PartialResultSet {
    const stats = {
      rowCountExact: rowCount,
      rowCount: 'rowCountExact',
    };
    return protobuf.PartialResultSet.create({
      stats,
    });
  }

  executeBatchDml(
    call: grpc.ServerUnaryCall<protobuf.ExecuteBatchDmlRequest>,
    callback: protobuf.Spanner.ExecuteBatchDmlCallback
  ) {
    callback(
      createUnimplementedError('ExecuteBatchDml is not yet implemented')
    );
  }

  read(
    call: grpc.ServerUnaryCall<protobuf.ReadRequest>,
    callback: protobuf.Spanner.ReadCallback
  ) {
    callback(createUnimplementedError('Read is not yet implemented'));
  }

  streamingRead(call: grpc.ServerWritableStream<protobuf.ReadRequest>) {
    call.destroy(
      createUnimplementedError('StreamingRead is not yet implemented')
    );
  }

  beginTransaction(
    call: grpc.ServerUnaryCall<protobuf.BeginTransactionRequest>,
    callback: protobuf.Spanner.BeginTransactionCallback
  ) {
    const session = this.sessions.get(call.request.session);
    if (session) {
      let counter = this.transactionCounters.get(session.name);
      if (!counter) {
        counter = 0;
      }
      const id = counter++;
      this.transactionCounters.set(session.name, counter);
      const transactionId = id.toString().padStart(12, '0');
      const fullTransactionId = session.name + '/transactions/' + transactionId;
      const readTimestamp =
        call.request.options && call.request.options.readOnly
          ? now()
          : undefined;
      const transaction = protobuf.Transaction.create({
        id: Buffer.from(transactionId),
        readTimestamp,
      });
      this.transactions.set(fullTransactionId, transaction);
      this.transactionOptions.set(fullTransactionId, call.request.options);
      callback(null, transaction);
    } else {
      callback(MockSpanner.createSessionNotFoundError(call.request.session));
    }
  }

  commit(
    call: grpc.ServerUnaryCall<protobuf.CommitRequest>,
    callback: protobuf.Spanner.CommitCallback
  ) {
    const session = this.sessions.get(call.request.session);
    if (session) {
      const buffer = Buffer.from(call.request.transactionId);
      const transactionId = buffer.toString();
      const fullTransactionId = session.name + '/transactions/' + transactionId;
      const transaction = this.transactions.get(fullTransactionId);
      if (transaction) {
        this.transactions.delete(fullTransactionId);
        this.transactionOptions.delete(fullTransactionId);
        callback(
          null,
          protobuf.CommitResponse.create({
            commitTimestamp: now(),
          })
        );
      } else {
        callback(MockSpanner.createTransactionNotFoundError(fullTransactionId));
      }
    } else {
      callback(MockSpanner.createSessionNotFoundError(call.request.session));
    }
  }

  rollback(
    call: grpc.ServerUnaryCall<protobuf.RollbackRequest>,
    callback: protobuf.Spanner.RollbackCallback
  ) {
    const session = this.sessions.get(call.request.session);
    if (session) {
      const buffer = Buffer.from(call.request.transactionId);
      const transactionId = buffer.toString();
      const fullTransactionId = session.name + '/transactions/' + transactionId;
      const transaction = this.transactions.get(fullTransactionId);
      if (transaction) {
        this.transactions.delete(fullTransactionId);
        this.transactionOptions.delete(fullTransactionId);
        callback(null, google.protobuf.Empty.create());
      } else {
        callback(MockSpanner.createTransactionNotFoundError(fullTransactionId));
      }
    } else {
      callback(MockSpanner.createSessionNotFoundError(call.request.session));
    }
  }

  partitionQuery(
    call: grpc.ServerUnaryCall<protobuf.PartitionQueryRequest>,
    callback: protobuf.Spanner.PartitionQueryCallback
  ) {
    callback(createUnimplementedError('PartitionQuery is not yet implemented'));
  }

  partitionRead(
    call: grpc.ServerUnaryCall<protobuf.PartitionReadRequest>,
    callback: protobuf.Spanner.PartitionReadCallback
  ) {
    callback(createUnimplementedError('PartitionQuery is not yet implemented'));
  }
}

/**
 * Creates and adds a MockSpanner instance to the given server. The MockSpanner instance does not contain any mocked results.
 */
export function createMockSpanner(server: grpc.Server): MockSpanner {
  const mock = MockSpanner.create();
  server.addService(spannerProtoDescriptor.Spanner.service, {
    batchCreateSessions: mock.batchCreateSessions,
    createSession: mock.createSession,
    getSession: mock.getSession,
    listSessions: mock.listSessions,
    deleteSession: mock.deleteSession,
    executeSql: mock.executeSql,
    executeStreamingSql: mock.executeStreamingSql,
    executeBatchDml: mock.executeBatchDml,
    read: mock.read,
    streamingRead: mock.streamingRead,
    beginTransaction: mock.beginTransaction,
    commit: mock.commit,
    rollback: mock.rollback,
    partitionQuery: mock.partitionQuery,
    partitionRead: mock.partitionRead,
  });
  return mock;
}

/**
 * Creates a simple result set containing the following data:
 *
 * |-----------------------------|
 * | NUM (INT64) | NAME (STRING) |
 * |-----------------------------|
 * |           1 | 'One'         |
 * |           2 | 'Two'         |
 * |           3 | 'Three'       |
 * -------------------------------
 *
 * This ResultSet can be used to easily mock queries on a mock Spanner server.
 */
export function createSimpleResultSet(): protobuf.ResultSet {
  const fields = [
    protobuf.StructType.Field.create({
      name: 'NUM',
      type: protobuf.Type.create({code: protobuf.TypeCode.INT64}),
    }),
    protobuf.StructType.Field.create({
      name: 'NAME',
      type: protobuf.Type.create({code: protobuf.TypeCode.STRING}),
    }),
  ];
  const metadata = new protobuf.ResultSetMetadata({
    rowType: new protobuf.StructType({
      fields,
    }),
  });
  return protobuf.ResultSet.create({
    metadata,
    rows: [
      {values: [{stringValue: '1'}, {stringValue: 'One'}]},
      {values: [{stringValue: '2'}, {stringValue: 'Two'}]},
      {values: [{stringValue: '3'}, {stringValue: 'Three'}]},
    ],
  });
}

/**
 * Returns a protobuf Timestamp containing the current local system time.
 */
export function now(): Timestamp {
  const now = Date.now();
  return Timestamp.create({seconds: now / 1000, nanos: (now % 1000) * 1e6});
}
