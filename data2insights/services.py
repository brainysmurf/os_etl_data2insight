from dataclasses import dataclass, field
import gspread
import google


@dataclass
class Service:
    creds: google.auth.credentials.Credentials = field(init=False)

    def __post_init__(self):
        self.local = False
        try:
            from google.colab import auth

        except ImportError:
            self.local = True
            from google.oauth2 import service_account
            from .sheets import GSheet

            self.creds = service_account.Credentials.from_service_account_file(
                "service_account.json", scopes=GSheet.scopes()
            )

        if not self.local:
            # This works with Google colab
            from google.auth import default

            auth.authenticate_user()
            self.creds, project = default()
