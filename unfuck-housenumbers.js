const console = require('better-console')
const co = require('co')
const pify = require('pify')
const path = require('path')
const fs = pify(require('fs'))

const {OUTPUT_DIRECTORY, sqlValue} = require('./lib/helpers')
const GeocodedAddresses = require('./data/GeocodedAddresses.json')
const GeocodedMissingAddresses = require('./data/GeocodedMissingAddresses.json')

function * run () {
  try {
    const Addresses = GeocodedAddresses.concat(GeocodedMissingAddresses)
    const query = []
    query.push(`BEGIN;`)
    Addresses
    .filter(AddressData => AddressData.address.country_code.toUpperCase() === 'GB')
    .filter(AddressData => AddressData.address.houseNumber)
    .forEach(AddressData => {
      const {email, address} = AddressData
      query.push('----------------------------------')
      query.push(`DO $func$`)
      // start the function
      query.push(`
DECLARE
  _person_id BIGINT;
  _address_id BIGINT;
BEGIN
      `)
      // look up the person
      query.push(`
  SELECT person_id INTO _person_id
  FROM people.email_address
  WHERE email=${sqlValue(email)};
      `)
      // make this conditional on finding a person
      query.push(`
    IF _person_id IS NOT NULL THEN
      `)
      // check if we already have this address for this person
      query.push(`
      SELECT id INTO _address_id
      FROM people.address
      WHERE person_id=_person_id AND google_place_id=${sqlValue(address.google_place_id)}
      AND house_number IS NULL;
      `)
      // add the address to the person
      query.push(`
      IF _address_id IS NOT NULL THEN
        UPDATE people.address
        SET house_number=${sqlValue(address.houseNumber)}
        WHERE id = _address_id;
      END IF;
      `)
      // end the _person_id NOT NULL conditional
      query.push(`
    END IF;
      `)
      // end the function
      query.push(`END $func$;`)
    })
    query.push(`COMMIT;`)

    // create output directory
    try {
      yield fs.mkdir(OUTPUT_DIRECTORY)
    } catch (err) {
      if (err.code !== 'EEXIST') {
        throw err
      }
    }

    // write to output directory
    yield fs.writeFile(path.join(OUTPUT_DIRECTORY, 'unfuck_missing_housenumbers.sql'), query.join(' \n'))
    console.log(`Wrote SQL query to output directory`)
  } catch (err) {
    console.error(err)
  }
}

co(run)
