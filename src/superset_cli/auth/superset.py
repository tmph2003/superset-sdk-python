"""
Mechanisms for authentication and authorization for Superset instances.
"""

from typing import Dict, Optional

from yarl import URL

from superset_cli.auth.main import Auth
from superset_cli.auth.token import TokenAuth


class UsernamePasswordAuth(Auth):  # pylint: disable=too-few-public-methods
    """
    Auth to Superset via username/password.
    """

    def __init__(self, baseurl: URL, username: str, password: Optional[str] = None):
        super().__init__()

        self.csrf_token: Optional[str] = None
        self.baseurl = baseurl
        self.username = username
        self.password = password
        self.auth()

    def get_csrf_token(self, jwt: str) -> str:
        """
        Get a CSRF token.
        """
        response = self.session.get(
            self.baseurl / "api/v1/security/csrf_token/",  # type: ignore
            headers={"Authorization": f"Bearer {jwt}"},
        )
        response.raise_for_status()
        payload = response.json()
        return payload["result"]

    def get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "X-CSRFToken": self.get_csrf_token(self.token),
        }

    def auth(self) -> None:
        """
        Login to get CSRF token and cookies.
        """
        data = {
            "username": self.username,
            "password": self.password,
            "provider": "db",
            "refresh": False
        }

        response = self.session.post(self.baseurl / "api/v1/security/login", json=data)
        if not response.ok:
            raise Exception(f"Failed to authenticate: {response.text}")
        elif "access_token" not in response.json():
            raise Exception(f"Authentication response did not contain access token: {response.text}")
        else:
            self.token = response.json()['access_token']



class SupersetJWTAuth(TokenAuth):  # pylint: disable=abstract-method
    """
    Auth to Superset via JWT token.
    """

    def __init__(self, token: str, baseurl: URL):
        super().__init__(token)
        self.baseurl = baseurl

    def get_csrf_token(self, jwt: str) -> str:
        """
        Get a CSRF token.
        """
        response = self.session.get(
            self.baseurl / "api/v1/security/csrf_token/",  # type: ignore
            headers={"Authorization": f"Bearer {jwt}"},
        )
        response.raise_for_status()
        payload = response.json()
        return payload["result"]

    def get_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "X-CSRFToken": self.get_csrf_token(self.token),
        }
