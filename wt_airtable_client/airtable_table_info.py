class AirtableTableInfo:
    """
    Information for retrieving data from a Table on Airtable
    """

    def __init__(
        self,
        name: str,
        id_column: str,
    ) -> None:
        """
        Construct AirtableTableInfo

        Args:
            name (str): Table name
            id_column (str): Name of identifier column
        """

        self.name = name
        self.id_column = id_column
