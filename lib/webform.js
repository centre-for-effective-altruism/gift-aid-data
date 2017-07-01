const knex = require('./db')
const console = require('better-console')
const moment = require('moment')
const {capitalizeName} = require('./helpers')

const webformIDMap = {
  304: 'GWWC',
  305: 'GW',
  306: 'GD',
  461: 'DMI',
  552: 'IPA',
  656: 'IGN'
}

function getWebformData (opts) {
  return getWebformComponents()
    .then(webformComponents => {
      const components = {}
      webformComponents.forEach(webformComponent => {
        components[webformComponent.nid] = components[webformComponent.nid] || {}
        components[webformComponent.nid][webformComponent.cid] = webformComponent.form_key
      })
      return getWebformSubmissions(opts)
        .then(webformSubmissions => {
          const submissions = {}
          webformSubmissions.forEach(webformSubmission => {
            submissions[webformSubmission.sid] = submissions[webformSubmission.sid] || {
              webform: webformIDMap[webformSubmission.nid],
              timestamp: webformSubmission.submitted
            }
            submissions[webformSubmission.sid][components[webformSubmission.nid][webformSubmission.cid]] = webformSubmission.data
          })
          return submissions
        })
        .then(submissionsMap => {
          const submissions = []
          Object.keys(submissionsMap).forEach(key => {
            submissions.push(submissionsMap[key])
          })
          return submissions
            .filter(submission => submission.email)
            .map(submission => normalizeWebformSubmission(submission))
        })
    })
}

function normalizeWebformSubmission (submission) {
  let normalizedSubmission = {
    firstName: submission.first_name ? capitalizeName(submission.first_name.trim()) : null,
    lastName: submission.last_name ? capitalizeName(submission.last_name.trim()) : null,
    email: submission.email ? submission.email.trim().toLowerCase() : null,
    address: submission.address ? submission.address.trim().replace(/\r\n/g, '\n') : null,
    webform: submission.webform || null,
    givingId: submission.givingid || null,
    datetime: moment.utc(submission.timestamp, 'X') || null,
    alternativeAllocation: submission.alternative && submission.alternative.trim().length ? submission.alternative.trim().replace(/\r\n/g, ',') : false,
    referrer: (submission.howheard || submission.heardfrom || '').trim() || null,
    charities: submission.charities || 'NONE'
  }
  // booleans
  const booleanKeys = ['shareinfo', 'contactinfo', 'giftaid', 'allocationchange']
  booleanKeys.forEach(key => {
    normalizedSubmission[key] = submission[key] === 'yes'
  })
  return normalizedSubmission
}

function getWebformComponents () {
  return knex.select('nid', 'cid', 'form_key', 'name')
    .from('webform_component')
    .whereIn('nid', Object.keys(webformIDMap))
}

function getWebformSubmissions (opts) {
  return knex
    .from('webform_submitted_data')
    .whereIn('webform_submitted_data.nid', Object.keys(webformIDMap))
    .leftJoin('webform_submissions', 'webform_submitted_data.sid', 'webform_submissions.sid')
    .select(['webform_submitted_data.nid', 'webform_submitted_data.sid', 'webform_submitted_data.cid', 'webform_submitted_data.data', 'webform_submissions.submitted'])
    // .then(rows => {
    //   return knex.destroy()
    //     .then(() => rows)
    // })
}

module.exports = {
  getWebformData
}
