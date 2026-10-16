from pydantic import BaseModel, Field

class DepositWithdrawForm(BaseModel):
    wallet_id: str
    amount: float = Field(gt=0, description="The amount must be greater than zero")

class TransferForm(BaseModel):
    source_wallet_id: str
    wallet_id: str  # Target wallet
    amount: float = Field(gt=0, description="The amount must be greater than zero")