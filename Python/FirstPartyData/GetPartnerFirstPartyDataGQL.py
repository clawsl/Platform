######################################################################
# This script calls GQL to get the first-party data for all advertisers
# under a specific partner, handling pagination at both advertiser and
# first-party data levels.
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

# Replace the placeholder with the ID of the partner you want to query advertisers for.
partner_id = 'PARTNER_ID_PLACEHOLDER'

# Replace the placeholder with the size(count) of the page size you'd like to return on a single query request.
advertiser_count = 100  # Number of advertisers per page
first_party_data_count = 1000  # Number of first-party data entries per page

# Replace the placeholder with a name filter you'd like to filter the return set on.
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
        # print(response.content)

    # Parse any data if it exists, otherwise, return an empty dictionary.
    resp_data = content.get('data', {})
    # Parse any errors if they exist, otherwise, return an empty error list.
    errors = content.get('errors', [])

    return (response.ok, GqlResponse(resp_data, errors))

# Retrieves a page of advertisers for the given partner.
def get_partner_advertisers(cursor: str) -> Any:
    query = """
    query GetPartnerAdvertisers($partnerId: ID!, $advertiserCount: Int, $advertiserCursor: String) {
        partner(id: $partnerId) {
            advertisers(first: $advertiserCount, after: $advertiserCursor) {
                nodes {
                    id
                    name
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    }"""

    variables = {
        "partnerId": partner_id,
        "advertiserCount": advertiser_count,
        "advertiserCursor": cursor
    }

    # Send the GraphQL request.
    request_success, response = execute_gql_request(query, variables)

    if not request_success:
        print(response.errors)
        raise Exception('Failed to fetch advertisers.')

    return response.data

# Retrieves a page of first-party data for a given advertiser.
def get_advertiser_first_party_data(advertiser_id: str, cursor: str) -> Any:
    query = """
    query GetAdvertiserFirstPartyData($advertiserId: ID!, $firstPartyDataCount: Int, $firstPartyDataCursor: String, $nameFilter: String) {
        advertiser(id: $advertiserId) {
            firstPartyData(first: $firstPartyDataCount, after: $firstPartyDataCursor, where: {name: {contains: $nameFilter}}) {
                nodes {
                    name
                    id
                    activeUniques {
                        householdCount
                        idsConnectedTvCount
                        idsCount
                        idsInAppCount
                        idsWebCount
                        personsCount
                    }
                }
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }
    }"""

    variables = {
        "advertiserId": advertiser_id,
        "firstPartyDataCount": first_party_data_count,
        "firstPartyDataCursor": cursor,
        "nameFilter": name_filter
    }

    # Send the GraphQL request.
    request_success, response = execute_gql_request(query, variables)

    if not request_success:
        print(response.errors)
        raise Exception(f'Failed to fetch first-party data for advertiser {advertiser_id}.')

    return response.data

# Queries all advertisers under a partner and retrieves their first-party data.
def query_partner_first_party_data() -> None:
    advertiser_has_next = True
    advertiser_cursor = None

    first_party_data_list = []

    while advertiser_has_next:
        print(f"Retrieving advertisers after cursor: {advertiser_cursor}")
        partner_data = get_partner_advertisers(advertiser_cursor)

        advertisers = partner_data['partner']['advertisers']['nodes']

        for advertiser in advertisers:
            advertiser_id = advertiser['id']
            advertiser_name = advertiser.get('name', 'Unknown Advertiser')
            print(f"\nRetrieving first-party data for advertiser {advertiser_name} (ID: {advertiser_id})")

            first_party_has_next = True
            first_party_cursor = None

            while first_party_has_next:
                print(f"  Retrieving first-party data after cursor: {first_party_cursor}")
                first_party_data_response = get_advertiser_first_party_data(advertiser_id, first_party_cursor)

                first_party_data_nodes = first_party_data_response['advertiser']['firstPartyData']['nodes']
                
                # Annotate each first-party data entry with the advertiser ID and name
                for node in first_party_data_nodes:
                    node['advertiserId'] = advertiser_id
                    node['advertiserName'] = advertiser_name
                    first_party_data_list.append(node)

                first_party_has_next = first_party_data_response['advertiser']['firstPartyData']['pageInfo']['hasNextPage']
                first_party_cursor = first_party_data_response['advertiser']['firstPartyData']['pageInfo']['endCursor']

            print(f"  Completed retrieving first-party data for advertiser {advertiser_name} (ID: {advertiser_id})")

        advertiser_has_next = partner_data['partner']['advertisers']['pageInfo']['hasNextPage']
        advertiser_cursor = partner_data['partner']['advertisers']['pageInfo']['endCursor']

    # Write to a text file using json.dump with indentation
    with open('output.txt', 'w') as file:
        json.dump(first_party_data_list, file, indent=4)

###########################################################
# Execution Flow:
#  1. Query all advertisers under the specified partner ID.
#  2. For each advertiser, query their first-party data.
#  3. Handle pagination at both advertiser and first-party data levels.
#  4. Write out the combined result to output.txt.
###########################################################
query_partner_first_party_data()