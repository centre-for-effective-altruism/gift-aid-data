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
      WHERE person_id=_person_id AND google_place_id=${sqlValue(address.google_place_id)};
      `)
      // add the address to the person
      query.push(`
      IF _address_id IS NULL THEN
        INSERT INTO people.address (
          person_id,
          house_number,
          address_line_1,
          address_line_2,
          city,
          region,
          postal_code,
          country_code,
          coordinates,
          google_place_id
        ) VALUES (
          _person_id,
          ${sqlValue(address.house_number)},
          ${sqlValue(address.address_line_1)},
          ${sqlValue(address.address_line_2)},
          ${sqlValue(address.city)},
          ${sqlValue(address.region)},
          ${sqlValue(address.postal_code)},
          ${sqlValue(address.country_code.toUpperCase())},
          postgis.ST_MakePoint(${sqlValue(address.latitude)}, ${sqlValue(address.longitude)}),
          ${sqlValue(address.google_place_id)}
        )
        RETURNING id INTO _address_id;
      END IF;
      `)
      // create gift aid claims for any payments that don't already have them
      query.push(`
      INSERT INTO payments.gift_aid_claim (payment_id, person_id, address_id)
      (
        SELECT payment.id, _person_id, _address_id
        FROM payments.payment
        WHERE metadata->>'gift_aid' IS NULL
        AND person_id = _person_id
      )
      ON CONFLICT DO NOTHING;
      `)
      // add the address to any recurring payments that have been made
      query.push(`
      UPDATE payments.recurring_payment
      SET metadata = COALESCE(metadata, '{}'::JSONB) || ('{ "gift_aid": { "claimed": true, "address_id": "' || _address_id || '" } }')::JSONB
      WHERE person_id = _person_id
      AND metadata->>'gift_aid' IS NULL;
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
    yield fs.writeFile(path.join(OUTPUT_DIRECTORY, 'giftaid_addresses.sql'), query.join(' \n'))
    console.log(`Wrote SQL query to output directory`)
  } catch (err) {
    console.error(err)
  }
}

co(run)
