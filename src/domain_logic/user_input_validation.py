from enum import Enum
from pydantic import field_validator, BaseModel, PositiveFloat, PositiveInt, ValidationInfo, ValidationError


class FilingStatus(str, Enum):
    single = 'single'
    married_filing_jointly = 'married filing jointly'
    married_filing_separately = 'married filing separately'
    head_of_household = 'head of household'


class UserTaxInfo(BaseModel):
    state: str
    income: PositiveFloat
    filing_status: FilingStatus

    @field_validator('state')
    @classmethod
    def valid_state(cls, st: str) -> str:
        valid_states = [ 'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut',
                         'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois',
                         'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts',
                         'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada',
                         'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota',
                         'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina',
                         'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington',
                         'West Virginia', 'Wisconsin', 'Wyoming' ]
        if st not in valid_states:
            raise ValueError(f'{st} is not a valid state. Must be one of 50 US states.')
        return st


class RothConversionLimits(BaseModel):
    limit: PositiveFloat
    filing_status: FilingStatus


class RothConversionInfo(BaseModel):
    retirement_balance: PositiveFloat
    conversion_amount: PositiveFloat
    withdrawal_age: PositiveInt
    withdrawal_age_income: PositiveFloat

    @field_validator('conversion_amount')
    @classmethod
    def conversion_amount_limit(cls, amount: PositiveFloat, info: ValidationInfo) -> PositiveFloat:
        if 'retirement_balance' in info.data and amount > info.data[ "retirement_balance" ]:
            raise ValueError(f'Roth conversion amount of {amount} cannot be greater than retirement balance '
                             f'of {info.data[ "retirement_balance" ]}')
        return amount

# try:
#     RothConversionInfo(retirement_balance=10000, conversion_amount=15000,
#                        withdrawal_age=72, withdrawal_age_income=25000)
# except ValidationError as err:
#     print(err.json(indent=4))

# try:
#     UserTaxInfo(state="TN", income=75000, filing_status='married filing jointly')
# except ValidationError as err:
#     print(err.json(indent=4))
