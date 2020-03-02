require('dotenv').config();
const knex = require('knex');
const _ = require('lodash');
const chalk = require('chalk');

const date = () => {
  const currentdate = new Date();
  return (
    currentdate.getDate() +
    '/' +
    (currentdate.getMonth() + 1) +
    '/' +
    currentdate.getFullYear() +
    ' @ ' +
    currentdate.getHours() +
    ':' +
    currentdate.getMinutes() +
    ':' +
    currentdate.getSeconds()
  );
};

const clientLog = (msg) => {
  if (msg) console.log(chalk.blue(`[Client] [${date()}] ${msg}`));
};

const serverLog = (msg) => {
  if (msg) console.log(chalk.red(`[Server] [${date()}] ${msg}`));
};

const neutralLog = (msg) => {
  if (msg) console.log(chalk.yellow(`[${date()}] ${msg}`));
};

const nullFields = {
  user_id: null,
  phone: null,
  street: null,
  postal_code: null,
  locality: null,
  country_id: null,
  company_id: null,
  note: null
};

const testConnection = async (instance) => {
  try {
    await instance.raw('select 1+1 as result');
  } catch (err) {
    console.log(err);
    process.exit(1);
  }
};

(async () => {
  const server = knex({
    client: 'mysql',
    connection: {
      host: process.env.SERVER_HOST,
      user: process.env.SERVER_USER,
      password: process.env.SERVER_PASSWORD,
      database: process.env.SERVER_DATABASE
    }
  }).on('query', (query) => {
    serverLog(query.sql);
  });

  const client = knex({
    client: 'mysql',
    connection: {
      host: process.env.CLIENT_HOST,
      user: process.env.CLIENT_USER,
      password: process.env.CLIENT_PASSWORD,
      database: process.env.CLIENT_DATABASE
    }
  }).on('query', (query) => {
    clientLog(query.sql);
  });

  await testConnection(server);
  await testConnection(client);

  neutralLog(`Sync every ${process.env.SYNC_GAP_MINUTES} minutes`);

  const sync = async () => {
    const serverRows = await server('etu_users')
      .select('firstName', 'lastName', 'login', 'mail')
      .whereRaw('bdeMembershipEnd > NOW()');

    const clientRows = await client('persons')
      .select('nickname AS login')
      .whereNull('user_id');

    const peopleToAdd = _.differenceBy(serverRows, clientRows, 'login');
    const peopleToRemove = _.differenceBy(clientRows, serverRows, 'login');

    if (peopleToAdd.length > 0) {
      await client('persons').insert(
        peopleToAdd.map((user) => ({
          first_name: user.firstName,
          last_name: user.lastName,
          nickname: user.login,
          email: user.mail,
          ...nullFields,
          created_at: client.fn.now(),
          updated_at: client.fn.now()
        }))
      );

      const ids = await client('persons')
        .select('id')
        .whereIn(
          'nickname',
          peopleToAdd.map((user) => user.login)
        );

      await client('taggables').insert(
        ids.map((row) => ({
          tag_id: process.env.TAG_ID,
          taggable_type: 'Robert2\\API\\Models\\Person',
          taggable_id: row.id
        }))
      );
    }
    if (peopleToRemove.length > 0) {
      const ids = await client('persons')
        .select('id')
        .whereIn(
          'nickname',
          peopleToRemove.map((user) => user.login)
        )
        .map((row) => row.id);
      await client('persons')
        .whereIn(
          'nickname',
          peopleToRemove.map((user) => user.login)
        )
        .del();
      await client('taggables')
        .where({ taggable_type: 'Robert2\\API\\Models\\Person' })
        .whereIn('taggable_id', ids)
        .del();
    }

    neutralLog(`${peopleToAdd.length} users added`);
    neutralLog(`${peopleToRemove.length} users removed`);
  };
  sync();
  setInterval(sync, process.env.SYNC_GAP_MINUTES * 60 * 1000);
})();
