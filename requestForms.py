from pydantic import BaseModel, Field

class DepositWithdrawForm(BaseModel):
    amount: float = Field(gt=0, description="The amount must be greater than zero")

class TransferForm(BaseModel):
    wallet_id: str
    amount: float = Field(gt=0, description="The amount must be greater than zero")