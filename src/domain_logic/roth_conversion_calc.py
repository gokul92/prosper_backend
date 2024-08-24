import numpy as np
import pandas as pd
from tax_rates_calc import IncomeTaxRates

"""
Assumes tax bracket at withdrawal is the same as current tax bracket - Eventually provide a way for user to input their income at withdrawal
Assumes 7000 is annual IRA contribution limit
Assumes 11% is the annual return on investments - Eventually fetch this from Exa ai.
"""

class RothConversion:
    def __init__(self, retirement_balance, income, filing_status, state, current_age, retirement_age):
        self.retirement_balance = retirement_balance
        self.income = income
        self.filing_status = filing_status
        self.state = state
        self.years_to_withdrawal = retirement_age - current_age + 1
        self.tax_rates = IncomeTaxRates(state, income, filing_status)
        self.income_tax_rate = self.tax_rates.calculate_income_tax_rate()["total_rate"]
        self.ltcg_rate = self.tax_rates.calculate_ltcg_rate()["total_rate"]

    def can_contribute_directly(self):
        roth_df = pd.read_csv('data/Backdoor Roth IRA conversion limits.csv')
        roth_limit = roth_df.loc[roth_df['filing_status'] == self.filing_status, 'limit'].values[0]
        return self.income <= roth_limit

    def calculate_conversion_amount(self):
        return min(7000, self.retirement_balance)  # Assuming $7000 is the annual IRA contribution limit

    def calculate_roth_balance_at_withdrawal(self, annual_contribution):
        return np.fv(0.11, self.years_to_withdrawal, -annual_contribution, 0)

    def calculate_traditional_balance_at_withdrawal(self, annual_contribution):
        fv = np.fv(0.11, self.years_to_withdrawal, -annual_contribution, 0)
        taxes = fv * self.income_tax_rate
        return fv - taxes

    def calculate_taxable_account_balance_at_withdrawal(self, annual_contribution):
        fv = np.fv(0.11, self.years_to_withdrawal, -annual_contribution, 0)
        gains = fv - (annual_contribution * self.years_to_withdrawal)
        taxes = gains * self.ltcg_rate
        return fv - taxes

    def calculate_savings(self, is_annual=False):
        conversion_amount = self.calculate_conversion_amount()
        
        if is_annual:
            roth_balance = self.calculate_roth_balance_at_withdrawal(conversion_amount)
            traditional_balance = self.calculate_traditional_balance_at_withdrawal(conversion_amount)
            taxable_balance = self.calculate_taxable_account_balance_at_withdrawal(conversion_amount)
        else:
            roth_balance = self.calculate_roth_balance_at_withdrawal(conversion_amount) / self.years_to_withdrawal
            traditional_balance = self.calculate_traditional_balance_at_withdrawal(conversion_amount) / self.years_to_withdrawal
            taxable_balance = self.calculate_taxable_account_balance_at_withdrawal(conversion_amount) / self.years_to_withdrawal

        roth_savings = roth_balance - taxable_balance
        traditional_savings = roth_balance - traditional_balance

        return {
            "roth_balance": roth_balance,
            "traditional_savings": traditional_savings,
            "taxable_savings": roth_savings,
            "taxes_paid_today": conversion_amount * self.income_tax_rate
        }

def calculate_roth_conversion_benefits(retirement_balance, income, filing_status, state, current_age, retirement_age):
    roth_conversion = RothConversion(retirement_balance, income, filing_status, state, current_age, retirement_age)
    
    can_contribute_directly = roth_conversion.can_contribute_directly()
    one_time_savings = roth_conversion.calculate_savings(is_annual=False)
    annual_savings = roth_conversion.calculate_savings(is_annual=True)

    return {
        "can_contribute_directly": can_contribute_directly,
        "one_time_conversion": one_time_savings,
        "annual_conversion": annual_savings
    }