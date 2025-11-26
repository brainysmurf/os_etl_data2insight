from google.colab import auth
from google.auth import default
import gspread


class GSheet:
    """
    Interact with a spreadsheet
    """

    def __init__(self, spreadsheet_id: str, open_: bool = True):
        """
        Authenticate with google and authorize utiltites
        spreadsheet_id: The ID from the URL of the spreadsheet

        -> Will have .document
        """
        self.spreadsheet_id = spreadsheet_id
        auth.authenticate_user()
        self.creds, _ = default()
        gc = gspread.authorize(self.creds)
        if open_:
            self.open_by_key()

    def open_by_key(self):
        self.document = gc.open_by_key(self.spreadsheet_id)
        return self.document
