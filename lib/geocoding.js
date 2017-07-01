require('dotenv').load()
const {GOOGLE_MAPS_GEOCODING_API_KEY} = process.env
const pify = require('pify')
const co = require('co')
const console = require('better-console')
const GoogleMaps = require('@google/maps')
const googleMapsClient = pify(GoogleMaps.createClient({
  key: GOOGLE_MAPS_GEOCODING_API_KEY
}))

function * geocodeAddress (address) {
  const geocodeData = yield googleMapsClient.geocode({address})
  if (geocodeData.status !== 200) throw new Error(geocodeData.error_message)
  return geocodeData.json.results
}

function normalizeAddress (place, addressString) {
  const addressStringParts = addressString.split(',').map(a => a.trim()) 
  const address = {
    latitude: place.geometry ? place.geometry.location.lat : null,
    longitude: place.geometry ? place.geometry.location.lng : null,
    googlePlaceId: place.place_id,
    formattedAddress: place.formatted_address
  }
  place.address_components.forEach(component => {
    if (['country'].includes(component.types[0])) {
      address[component.types[0]] = component.short_name
    } else {
      address[component.types[0]] = component.long_name
    }
  })

  const formattedAddress = {}
  if (address.premise || address.subpremise) {
    if (address.premise && address.subpremise) {
      // street number
      formattedAddress.house_number = address.subpremise
      // address lines
      formattedAddress.address_line_1 = address.premise
      formattedAddress.address_line_2 = [address.street_number, address.route || address.intersection || addressStringParts[0]].join(' ').trim()
    } else {
      formattedAddress.house_number = address.subpremise || address.premise
      formattedAddress.address_line_1 = [address.street_number, address.route || address.intersection || addressStringParts[0]].join(' ').trim()
    }
  } else {
    // street number
    formattedAddress.house_number = address.street_number
    // address lines
    formattedAddress.address_line_1 = address.route || address.intersection || addressStringParts[0]
  }

  // Extract house number from route if it didn't show up individually
  const houseNumberRegex = /^(\d+\w*)\s(.*)$/
  if (!formattedAddress.house_number && formattedAddress.address_line_1) {
    const matches = formattedAddress.address_line_1.match(houseNumberRegex)
    if (matches && matches[1]) {
      formattedAddress.house_number = matches[1]
      formattedAddress.address_line_1 = matches[2]
    }
  }
  // city
  formattedAddress.city = address.neighborhood || address.colloquial_area || address.locality || address.postal_town || address.administrative_area_level_2
  // region
  formattedAddress.region = ['GB'].includes(address.country) ? address.administrative_area_level_2 : address.administrative_area_level_1
  // postal code
  formattedAddress.postal_code = address.postal_code || address.postal_code_prefix
  // country code
  formattedAddress.country_code = address.country
  // coordinates
  formattedAddress.latitude = address.latitude
  formattedAddress.longitude = address.longitude
  // place ID
  formattedAddress.google_place_id = address.googlePlaceId
  return formattedAddress
}

module.exports = {
  geocodeAddress: co.wrap(geocodeAddress),
  normalizeAddress
}
