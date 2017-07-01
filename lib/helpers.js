const moment = require('moment')
const regexNamedGroups = require('regex-named-groups')
const path = require('path')

// constants

const DATA_DIRECTORY = path.join(__dirname, '..', 'data')
const OUTPUT_DIRECTORY = path.join(__dirname, '..', 'output')

// get all weeks between a start and end date
function getWeeksInRange (startMoment, endMoment) {
  let currentMoment = startMoment
  const weeks = []
  while (currentMoment.isBefore(endMoment)) {
    weeks.push(currentMoment.twix(moment(currentMoment).add(6, 'days'), {allDay: true}))
    currentMoment.add(1, 'week')
  }
  return weeks
}

function capitalizeName (name) {
  const chars = name.split('')
  const capitalizedName = []
  chars.forEach((char, index) => {
    if (index === 0 || /[^A-Za-z\u00C0-\u017F]/.test(chars[index - 1])) {
      capitalizedName.push(char.toUpperCase())
    } else {
      capitalizedName.push(char.toLowerCase())
    }
  })
  return capitalizedName.join('')
}

function splitName (fullName) {
  const firstName = fullName.split(/\s/).filter(a => a)
  const lastName = firstName.splice(-1, 1)
  return {
    firstName: firstName.join(' '),
    lastName: lastName[0]
  }
}

function extractHouseNumber (address) {
  if (!address) return null
  const re = /((?:flat|apt|apartment|unit|level)[\s][\d]+[\w]*)|([\d]+[\w]*)/gim
  const namedRegex = regexNamedGroups(re, ['flat', 'number'])
  const match = namedRegex.exec(address)
  if (!match) return null
  return (match.flat || match.number || '').trim() || null
}

const ukPostcodeRegex = /(([gG][iI][rR] {0,}0[aA]{2})|((([a-pr-uwyzA-PR-UWYZ][a-hk-yA-HK-Y]?[0-9][0-9]?)|(([a-pr-uwyzA-PR-UWYZ][0-9][a-hjkstuwA-HJKSTUW])|([a-pr-uwyzA-PR-UWYZ][a-hk-yA-HK-Y][0-9][abehmnprv-yABEHMNPRV-Y]))) {0,}[0-9][abd-hjlnp-uw-zABD-HJLNP-UW-Z]{2}))/

function extractPostcode (address) {
  if (!address) return null
  // taken from http://stackoverflow.com/a/7259020
  const match = address.match(ukPostcodeRegex)
  if (!match || !match.length) return null
  const sanitized = match[0].toUpperCase().replace(/\s/g, '')
  return `${sanitized.substr(0, sanitized.length - 3)} ${sanitized.substr(sanitized.length - 3)}`
}

function sqlValue (value) {
  // integer
  if (!value) return 'NULL'
  if (value === Infinity) return `$inf$infinity$inf$`
  if (typeof value === 'number') return value
  if (typeof value === 'object') return `$obj$${JSON.stringify(value)}$obj$`
  if (typeof value === 'string') return `$str$${value}$str$`
}

module.exports = {
  DATA_DIRECTORY,
  OUTPUT_DIRECTORY,
  ukPostcodeRegex,
  getWeeksInRange,
  capitalizeName,
  splitName,
  extractHouseNumber,
  extractPostcode,
  sqlValue
}
