######################################################################
# This script calls GQL to get the first party data for an advertiser.
######################################################################

import json
import requests
from typing import Any, List, Tuple

###########
# Constants
###########

# Define the GQL Platform API endpoint URLs.
EXTERNAL_SB_GQL_URL = 'https://ext-desk.sb.thetradedesk.com/graphql'
PROD_GQL_URL = 'https://desk.thetradedesk.com/graphql'

#############################
# Variables for YOU to define
#############################

# Define the GraphQL Platform API endpoint URL this script will use.
gql_url = EXTERNAL_SB_GQL_URL

# Replace the placeholder value with your actual API token.
token = 'TOKEN_PLACEHOLDER'

# Replace the placeholder with the ID of the advertiser you want to query first party data for.
advertiser_id = 'ADVERTISER_ID_PLACEHOLDER'

# Replace the placeholder with the size(count) of the page size you'd like to return on a single query request.
count = 1000

# Replace the placerholder with a name filter you'd like to filter the return set on.
name_filter = 'NAME_PLACEHOLDER'

################
# Helper Methods
################

# Represents a response from the GQL server.
class GqlResponse:
  def __init__(self, data: dict[Any, Any], errors: List[Any]) -> None:
    # This is where the return data from the GQL operation is stored.
    self.data = data
    # This is where any errors from the GQL operation are stored.
    self.errors = errors

# Executes a GQL request to the specified gql_url, using the provided body definition and associated variables.
# This indicates if the call was successful and returns the `GqlResponse`.
def execute_gql_request(body, variables) -> Tuple[bool, GqlResponse]:
  # Create headers with the authorization token.
  headers: dict[str, str] = {
    'TTD-Auth': token
  }

  # Create a dictionary for the GraphQL request.
  data: dict[str, Any] = {
    'query': body,
    'variables': variables
  }

  # Send the GraphQL request.
  response = requests.post(url=gql_url, json=data, headers=headers)
  content = json.loads(response.content) if len(response.content) > 0 else {}

  if not response.ok:
    print('GQL request failed!')
    # For more verbose error messaging, uncomment the following line:
    #print(response)

  # Parse any data if it exists, otherwise, return an empty dictionary.
  resp_data = content.get('data', {})
  # Parse any errors if they exist, otherwise, return an empty error list.
  errors = content.get('errors', [])

  return (response.ok, GqlResponse(resp_data, errors))


# A GQL query to retrieve advertiser's first party data based off an offset.
def get_advertiser_first_party_data(cursor: str) -> Any:

  after_clause = f'after: "{cursor}",' if cursor else ''
  query = f"""
  query GetFirstPartyData($advertiserId: ID!, $count: Int, $nameFilter: String) {{
    advertiser(id: $advertiserId) {{
      firstPartyData({after_clause} first: $count, where:{{name:{{contains:$nameFilter}}}}){{
        nodes{{
            name
            id
            activeUniques{{
             householdCount
             idsConnectedTvCount
             idsCount
             idsInAppCount
             idsWebCount
             personsCount
            }}
        }}
        pageInfo{{
            hasNextPage
            endCursor
        }}
        }}
    }}
  }}"""

  # Define the variables in the query.
  variables = {
    "advertiserId": advertiser_id,
    "count": count,
    "nameFilter": name_filter
  }

  # Send the GraphQL request.
  request_success, response = execute_gql_request(query,variables)

  if not request_success:
    print(response.errors)
    raise Exception('Failed to fetch advertisers.')

  return response.data

# Queries a given advertiser's first party data by advertiserId and prints the result.
def query_advertiser() -> None:
  has_next = True
  cursor = None

  first_party_data = []
  while has_next:
    print(f"Retrieving advertiser first party data after cursor: {cursor}")
    advertiser_data = get_advertiser_first_party_data(cursor)

    # Retrieve advertiser IDs.
    for node in advertiser_data['advertiser']['firstPartyData']['nodes']:
      first_party_data.append(node)

    # Update pagination information.
    has_next = advertiser_data['advertiser']['firstPartyData']['pageInfo']['hasNextPage']
    cursor = advertiser_data['advertiser']['firstPartyData']['pageInfo']['endCursor']

  # Write to a text file using json.dump with indentation
  with open('output.txt', 'w') as file:
      json.dump(first_party_data, file, indent=4)



###########################################################
# Execution Flow:
#  1. Query the advertiser ID specified for first party data.
#  2. Keep querying until there are no more pages left to query.
#  3. Write out the result to output.txt.
###########################################################
query_advertiser()