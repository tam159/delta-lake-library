"""Python helper utilities."""


class StringConversion:
    """String conversion between camel case and snake case."""

    @staticmethod
    def camel_to_snake(string: str) -> str:
        """
        Convert camel case string to snake case string.

        :param string: string will be converted
        :return: string in snake case
        """
        return "".join(["_" + i.lower() if i.isupper() else i for i in string]).lstrip(
            "_"
        )

    @staticmethod
    def snake_to_camel(string: str) -> str:
        """
        Convert snake case string to camel case string.

        :param string: string will be converted
        :return: string in camel case
        """
        return "".join([char.title() for char in string.split("_")])
