# -*- coding: utf-8 -*-
"""
SurveyMonkey API Client.
"""
import os
import logging
from functools import wraps
from urllib.parse import urlencode, urljoin

import requests
from requests import Session
from requests.adapters import HTTPAdapter, Retry


class SurveyMonkeyDailyRateLimitConsumed(Exception):
    pass


LOGGER = logging.getLogger(__name__)


# https://developer.surveymonkey.com/api/v3/?ut_source=header#headers
SURVEY_MONKEY_RATE_LIMITING_HEADERS = [
    "X-Ratelimit-App-Global-Day-Limit",
    "X-Ratelimit-App-Global-Day-Remaining",
    "X-Ratelimit-App-Global-Day-Reset",
    "X-Ratelimit-App-Global-Minute-Limit",
    "X-Ratelimit-App-Global-Minute-Remaining",
    "X-Ratelimit-App-Global-Minute-Reset",
]


def ensure_rate_limit_constraints(func):
    """
    Decorate to ensure respect for SurveyMonkey rate limit constraints.
    """
    response_headers = {}

    @wraps(func)
    def wrapper(*args, **kwargs):

        if response_headers:
            # check daily limit
            if response_headers["X-Ratelimit-App-Global-Day-Remaining"] == 0:
                max_limit = response_headers["X-Ratelimit-App-Global-Day-Limit"]
                remain = response_headers["X-Ratelimit-App-Global-Day-Remaining"]
                raise SurveyMonkeyDailyRateLimitConsumed(
                    f"Consumed daily api call limit. Can not make more calls. Max: [{max_limit}], Remaining: [{remain}]"
                )

        response = func(*args, **kwargs)
        headers = response.headers

        # store all rate limiting headers
        for header in SURVEY_MONKEY_RATE_LIMITING_HEADERS:
            response_headers[header] = int(headers.get(header))

        return response

    return wrapper


# https://requests.readthedocs.io/en/latest/user/authentication/#new-forms-of-authentication
class BearerAuth(requests.auth.AuthBase):
    """
    Bearer authentication class.
    """

    def __init__(self, access_token):
        """
        Initialize access token.
        """
        self.access_token = access_token

    def __call__(self, request):
        """
        Set `Authorization` header.
        """
        request.headers['Authorization'] = f'Bearer {self.access_token}'
        return request


class SurveyMonkeyApiClient:
    """
    SurveyMonkey client authenticates using a access token.
    """

    ACCESS_TOKEN = os.environ['SURVEYMONKEY_ACCESS_TOKEN']
    API_BASE_URL = 'https://api.surveymonkey.com/v3/'

    def __init__(self, survey_id, start_at=None):
        """
        Initialize the instance with arguments provided or default values otherwise.
        """
        self.client = Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        self.client.mount('https://', HTTPAdapter(max_retries=retries))
        self.client.auth = BearerAuth(self.ACCESS_TOKEN)

        self.survey_id = survey_id
        self.start_at = start_at

    def get_endpoint_url(self):
        """
        Construct the full API URL using the API_BASE_URL and path.

        Args:
            path (str): API endpoint path.
        """
        query_params = {
            'simple': True,
            'sort_order': 'ASC',
            'per_page': 100,
        }
        if self.start_at is not None:
            query_params['start_created_at'] = self.start_at

        query_params_encoded = urlencode(query_params)

        return urljoin(f"{self.API_BASE_URL}/", f'surveys/{self.survey_id}/responses/bulk?{query_params_encoded}')

    @ensure_rate_limit_constraints
    def fetch_survey_responses(self, api_url):
        """
        Maka a HTTP GET call to `api_url` and return response.
        """
        response = self.client.get(api_url)
        response.raise_for_status()
        return response
