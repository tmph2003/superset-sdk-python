"""
Mechanisms for authentication and authorization.
"""

from typing import Any, Dict

from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class Auth:  # pylint: disable=too-few-public-methods
    """
    An authentication/authorization mechanism.
    """

    def __init__(self):
        self.session = Session()
        self.session.hooks["response"].append(self.reauth)

        retries = Retry(
            total=3,  # max retries count
            backoff_factor=1,  # delay factor between attempts
            respect_retry_after_header=True,
        )

        self.session.mount("https://", HTTPAdapter(max_retries=retries))

    def get_headers(self) -> Dict[str, str]:
        """
        Return headers for auth.
        """
        return {}

    def auth(self) -> None:
        """
        Perform authentication, fetching JWT tokens, CSRF tokens, cookies, etc.
        """
        raise NotImplementedError("Must be implemented for reauthorizing")

    # pylint: disable=invalid-name, unused-argument
    def reauth(self, r: Response, *args: Any, **kwargs: Any) -> Response:
        """
        Catch 401 and re-auth.
        """
        if r.status_code != 401:
            return r

        try:
            self.auth()
        except NotImplementedError:
            return r

        self.session.headers.update(self.get_headers())
        r.request.headers.update(self.get_headers())
        return self.session.send(r.request, verify=False)
