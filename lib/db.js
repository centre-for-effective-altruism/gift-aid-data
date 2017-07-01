const Knex = require('knex')
const knex = new Knex({
  client: 'mysql2',
  connection: {
    host: 'localhost',
    user: 'root',
    password: 'root',
    database: 'gwwc_drupal'
  }
})

module.exports = knex
