"""
This module calculates income tax rates and long-term capital gains tax rates
based on state, income, and filing status.
"""
from typing import Dict, Union, List
import numpy as np
import pandas as pd
import os


class IncomeTaxRates:
    def __init__(self, state: str, income: float, filing_status: str):
        """
        Initialize the IncomeTaxRates class.

        Args:
            state (str): The state for tax calculation.
            income (float): The income amount.
            filing_status (str): The filing status (e.g., 'single', 'married filing jointly').
        """
        self.state = state
        self.income = income
        self.filing_status = filing_status.lower()

    def _load_tax_data(self, filename: str) -> pd.DataFrame:
        """
        Load tax data from a CSV file.

        Args:
            filename (str): The name of the CSV file.

        Returns:
            pd.DataFrame: The loaded tax data.

        Raises:
            FileNotFoundError: If the specified file is not found.
        """
        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(current_dir, '..', '..', 'data')
        file_path = os.path.join(data_dir, filename)
        
        try:
            df = pd.read_csv(file_path)
            df.loc[df['income_hl'].isnull(), 'income_hl'] = np.inf
            return df
        except FileNotFoundError:
            raise FileNotFoundError(f"Tax data file not found: {file_path}")
        
    def _load_state_ltcg_data(self, filename: str) -> pd.DataFrame:
        """
        Load state LTCG data from a CSV file.

        Args:
            filename (str): The name of the CSV file.

        Returns:
            pd.DataFrame: The loaded state LTCG data.

        Raises:
            FileNotFoundError: If the specified file is not found.
        """
        current_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(current_dir, '..', '..', 'data')
        file_path = os.path.join(data_dir, filename)
        
        try:
            df = pd.read_csv(file_path)
            df['ordinary_rate'] = pd.to_numeric(df['ordinary_rate'], errors='coerce')
            df['long_term_rate'] = pd.to_numeric(df['long_term_rate'], errors='coerce')
            return df
        except FileNotFoundError:
            raise FileNotFoundError(f"State LTCG data file not found: {file_path}")

    def _calculate_progressive_tax(self, brackets: List[Dict[str, Union[float, str]]]) -> float:
        """
        Calculate the effective tax rate using progressive tax logic.

        Args:
            brackets (List[Dict[str, Union[float, str]]]): List of tax brackets.

        Returns:
            float: The effective tax rate.
        """
        total_tax = 0
        remaining_income = self.income

        for bracket in brackets:
            bracket_income = min(remaining_income, bracket['income_hl'] - bracket['income_ll'])
            total_tax += bracket_income * bracket['tax_rate']
            remaining_income -= bracket_income

            if remaining_income <= 0:
                break

        return total_tax / self.income if self.income > 0 else 0

    def calculate_federal_income_tax_rate(self) -> float:
        """
        Calculate the effective federal income tax rate.

        Returns:
            float: The effective federal income tax rate.

        Raises:
            ValueError: If no matching tax bracket is found.
        """
        df = self._load_tax_data('2024 Federal Income Tax Rates and Brackets.csv')
        brackets = df[
            (df['filing_status'] == self.filing_status) &
            (df['income_ll'] <= self.income)
        ].sort_values('income_ll').to_dict('records')

        if not brackets:
            raise ValueError(f"No matching federal income tax bracket found for income {self.income} and filing status {self.filing_status}")

        return self._calculate_progressive_tax(brackets)

    def calculate_state_income_tax_rate(self) -> float:
        """
        Calculate the effective state income tax rate.

        Returns:
            float: The effective state income tax rate.

        Raises:
            ValueError: If no matching tax bracket is found.
        """
        df = self._load_tax_data('2024 State Tax Brackets.csv')
        brackets = df[
            (df['state'] == self.state) &
            (df['filing_status'] == self.filing_status) &
            (df['income_ll'] <= self.income)
        ].sort_values('income_ll').to_dict('records')

        if not brackets:
            raise ValueError(f"No matching state income tax bracket found for state {self.state}, income {self.income}, and filing status {self.filing_status}")

        return self._calculate_progressive_tax(brackets)

    def calculate_federal_ltcg_rate(self) -> float:
        """
        Calculate the effective federal long-term capital gains tax rate.

        Returns:
            float: The effective federal LTCG tax rate.

        Raises:
            ValueError: If no matching tax bracket is found.
        """
        df = self._load_tax_data('Federal Long Term Capital Gains.csv')
        brackets = df[
            (df['filing_status'] == self.filing_status) &
            (df['income_ll'] <= self.income)
        ].sort_values('income_ll').to_dict('records')

        if not brackets:
            raise ValueError(f"No matching federal LTCG tax bracket found for income {self.income} and filing status {self.filing_status}")

        return self._calculate_progressive_tax(brackets)

    def calculate_state_ltcg_rate(self) -> float:
        """
        Calculate the state long-term capital gains tax rate.

        Returns:
            float: The state LTCG tax rate.
        """
        df = self._load_state_ltcg_data('State Long Term Capital Gains.csv')
        if self.state in df['state'].values:
            return df.loc[df['state'] == self.state, 'long_term_rate'].values[0]
        return 0.0

    def calculate_income_tax_rate(self) -> Dict[str, float]:
        """
        Calculate the total income tax rate (federal + state).

        Returns:
            Dict[str, float]: A dictionary containing federal, state, and total income tax rates.
        """
        federal_rate = self.calculate_federal_income_tax_rate()
        state_rate = self.calculate_state_income_tax_rate()
        return {
            "federal_rate": federal_rate,
            "state_rate": state_rate,
            "total_rate": federal_rate + state_rate
        }

    def calculate_ltcg_rate(self) -> Dict[str, float]:
        """
        Calculate the total long-term capital gains tax rate (federal + state).

        Returns:
            Dict[str, float]: A dictionary containing federal, state, and total LTCG tax rates.
        """
        federal_rate = self.calculate_federal_ltcg_rate()
        state_rate = self.calculate_state_ltcg_rate()
        return {
            "federal_rate": federal_rate,
            "state_rate": state_rate,
            "total_rate": federal_rate + state_rate
        }