const console = require('better-console')
const co = require('co')
const _ = require('lodash')
const throat = require('throat')
const pify = require('pify')
const promiseRetry = require('promise-retry')
const {titleize} = require('inflect')
// const knex = require('./lib/db')
const {geocodeAddress, normalizeAddress} = require('./lib/geocoding')

const path = require('path')
const fs = pify(require('fs'))

const {DATA_DIRECTORY, ukPostcodeRegex, extractHouseNumber} = require('./lib/helpers')
const MissingAddresses = require('./data/MissingAddresses.json')

function * run () {
  try {
    const OverwrittenAddresses = []
    const Addresses = yield Promise.all(MissingAddresses.map(throat(10, co.wrap(function * (Webform) {
      const addressComponents = Webform.address.split(',').map(a => a.trim())
      console.info(`Geocoding ${Webform.address}...`)
      // start at the postcode
      const postcodeIndex = addressComponents.findIndex(a => ukPostcodeRegex.test(a))
      const addressStrings = []
      if (postcodeIndex > -1) {
        const postcodeComponent = addressComponents[postcodeIndex]
        const postcode = postcodeComponent.match(ukPostcodeRegex)[0]
        // start building a progressively longer string out of the address components
        if (postcodeComponent !== postcode) addressStrings.push(postcode) // start with the extracted postcode
      }
      const remainingComponents = addressComponents
        .slice(0, postcodeIndex > -1 ? (postcodeIndex + 1) : (addressComponents.length - 1))
        .reverse() // start at the end
      remainingComponents
        .forEach((component, index) => {
          addressStrings.push(remainingComponents.slice(0, index + 1).reverse().join(', '))
        })
      return Promise.all(addressStrings.filter(a => a).map(addressString => {
        return promiseRetry((retry) => {
          return geocodeAddress(addressString)
            .then(AddressData => AddressData[0])
            .catch(err => {
              console.warn(err)
              retry(err)
            })
        })
      }))
        .then(Places => Places.filter(Place => Place)) // filter nulls
        .then(Places => Places.length ? Places[Places.length - 1] : null) // get the last element, which should have the most complete address
        .then(Place => {
          if (!Place) return null
          const NormalizedAddress = normalizeAddress(Place, Webform.address)
          if (NormalizedAddress.address_line_2) OverwrittenAddresses.push({Webform, NormalizedAddress: Object.assign({}, NormalizedAddress)})
          // shuffle the first component of the address down into address line 2
          NormalizedAddress.address_line_2 = `${NormalizedAddress.house_number || ''} ${NormalizedAddress.address_line_1}`.trim()
          // infer address line 1 and house number from the webform
          NormalizedAddress.house_number = extractHouseNumber(addressComponents[0])
          NormalizedAddress.address_line_1 = NormalizedAddress.house_number
            ? titleize(addressComponents[0].slice((addressComponents[0].indexOf(NormalizedAddress.house_number) + NormalizedAddress.house_number.length)).trim())
            : titleize(addressComponents[0])
          // if it turns out we've got overlap between the address lines, get rid of address line 2
          if (NormalizedAddress.address_line_2.toLowerCase() === NormalizedAddress.address_line_1.toLowerCase()) NormalizedAddress.address_line_2 = null
          else if (NormalizedAddress.address_line_2.toLowerCase() === `${NormalizedAddress.house_number || ''} ${NormalizedAddress.address_line_1}`.trim().toLowerCase()) NormalizedAddress.address_line_2 = null
          // return with email address and normalized address
          return {
            email: Webform.email,
            address: NormalizedAddress
          }
        })
    }))))
      .then(Addresses => Addresses.filter(address => address))
      .then(Addresses => _.uniqWith(Addresses, (arrVal, othVal) => {
        // filter duplicates on email and place ID
        if (!arrVal.address.google_place_id) return false // don't compare undefined/nulls
        return (arrVal.email === othVal.email && arrVal.address.google_place_id === othVal.address.google_place_id)
      }))
    // write to data directory
    yield fs.writeFile(path.join(DATA_DIRECTORY, 'GeocodedMissingAddresses.json'), JSON.stringify(Addresses))
    console.log(`Wrote ${Addresses.length} addresses to data directory`)
    if (OverwrittenAddresses.length) {
      yield fs.writeFile(path.join(DATA_DIRECTORY, 'OverwrittenAddresses.json'), JSON.stringify(OverwrittenAddresses))
      console.warn(`${OverwrittenAddresses.length} may have been overwritten!`)
    }
  } catch (err) {
    console.error(err)
  } finally {
    // knex.destroy()
  }
}

co(run)
