from dataclasses import dataclass, field
import gspread
from .services import Service
import google
import pandas as pd
import logging

logger = logging.getLogger(__name__)


@dataclass
class Doc:
    spreadsheet: gspread.Spreadsheet

    @classmethod
    def convert_data_to_df(cls, data):
        return pd.DataFrame(data[1:], columns=data[0])  # first row as header

    def read_tab(self, title):
        tab = self.spreadsheet.worksheet(title)
        # 4. Get all values as a list of lists
        data = tab.get_all_values()
        logger.debug("Got data")

        # 5. Convert to a DataFrame
        return Doc.convert_data_to_df(data)


@dataclass
class GSheet:
    """
    Initialize a spreadsheet for convenience
    """

    service: Service
    spreadsheet_id: str  # req
    open_: bool = True  # open by default
    client: google.oauth2.service_account.Credentials = field(init=False)
    document: Doc = field(init=False)  # let me init

    def __post_init__(self, open_: bool = True):
        """
        Authenticate with google and authorize utiltites
        spreadsheet_id: The ID from the URL of the spreadsheet

        -> Will have .document
        """
        self.client = gspread.Client(auth=self.service.creds)
        if open_:
            self.open_by_key()

    def open_by_key(self) -> Doc:
        self.document = Doc(self.client.open_by_key(self.spreadsheet_id))
        return self.document

    @classmethod
    def scopes(cls):
        return ["https://www.googleapis.com/auth/spreadsheets"]
