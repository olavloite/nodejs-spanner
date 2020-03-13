// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

'use strict';

async function profileQuery(instanceId, databaseId, projectId) {
  // [START spanner_profile_query]
  // Imports the Google Cloud client library
  // TODO: change to published version when available.
  // eslint-disable-next-line node/no-unpublished-require
  const {Spanner} = require('../build/src/index');

  /**
   * TODO(developer): Uncomment the following lines before running the sample.
   */
  // const projectId = 'my-project-id';
  // const instanceId = 'my-instance';
  // const databaseId = 'my-database';

  // Creates a client
  const spanner = new Spanner({
    projectId: projectId,
  });

  // Gets a reference to a Cloud Spanner instance and database
  const instance = spanner.instance(instanceId);
  const database = instance.database(databaseId);

  const query = {
    sql: `SELECT AlbumId, AlbumTitle, MarketingBudget
          FROM Albums
          ORDER BY AlbumTitle`,
    queryMode: 'PROFILE',
  };

  try {
    const [rows, stats] = await database.run(query);
    rows.forEach(row => {
      const json = row.toJSON();
      const marketingBudget = json.MarketingBudget
        ? json.MarketingBudget
        : null; // This value is nullable
      console.log(
        `AlbumId: ${json.AlbumId}, AlbumTitle: ${json.AlbumTitle}, MarketingBudget: ${marketingBudget}`
      );
    });
    stats.queryPlan.planNodes.forEach(node => {
      console.log(JSON.stringify(node));
    });
  } catch (err) {
    console.error('ERROR:', err);
  } finally {
    // Close the database when finished.
    database.close();
  }
  // [END spanner_profile_query]
}

require(`yargs`)
  .demand(1)
  .command(
    `profileQuery <instanceName> <databaseName> <projectId>`,
    `Execute a query in PROFILE mode which will return both the query results, statistics and query plan`,
    {},
    opts => profileQuery(opts.instanceName, opts.databaseName, opts.projectId)
  )
  .example(`node $0 profileQuery "my-instance" "my-database" "my-project-id"`)
  .wrap(120)
  .recommendCommands()
  .epilogue(`For more information, see https://cloud.google.com/spanner/docs`)
  .strict()
  .help().argv;
