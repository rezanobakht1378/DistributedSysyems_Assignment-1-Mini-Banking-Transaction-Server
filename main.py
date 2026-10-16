from fastapi import FastAPI, HTTPException, status
from requestForms import DepositWithdrawForm, TransferForm

app = FastAPI()

#TODO: ADD logger with this fields:
# Request ID
# Client IP
# Action
# Account ID
# Timestamp
# Result
# Latency

@app.get("/balance/inquiry")
def read_balance():
    return {
        "wallet_id": "354654354",
        "balance": "22"
        }

@app.post("/deposit")
def deposit(request, body: DepositWithdrawForm):
    body = body.model_dump()
    if body is None:
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"Case":"Malformed request", "Example":"broken JSON or random bytes on the socket"})
    account = request.account
    account.balance += body.amount
    # create transaction with type deposit

@app.post("/withdraw")
def withdraw(request, body: DepositWithdrawForm):
    body = body.model_dump()
    if body is None:
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"Case":"Malformed request", "Example":"broken JSON or random bytes on the socket"})
        
    if not body.amount:
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"Case":"Missing fields","Example":"a withdraw request without amount"})
    
    account = request.account
    if account.balance >= body.amount:
        account.balance -= body.amount
    else:
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"Case": "Insufficient funds" , "Example":"withdrawing 5000 from an account holding 800"})
    # create transaction with type withdraw

@app.post("/transfer")
def transfer(request, body: TransferForm):
    body = body.model_dump()
    if body is None:
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"Case":"Malformed request", "Example":"broken JSON or random bytes on the socket"})
    
    target_account = Account.where("wallet",body.wallet_id).first()
    if not target_account:
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail={"Case":"Unknown account", "Example":"a request for account Z9999 which does not exist"})
    
    if not body.amount:
            return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"Case":"Missing fields","Example":"a withdraw request without amount"})
        
    if type(body.amount) != str or body.amount <= 0:
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"Case":"Negative or invalid amount","Example":"-50, 0, 'abc'"})