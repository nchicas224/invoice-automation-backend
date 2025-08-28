
import hashlib
import logging
from typing import Optional
from shared.database_client import get_db_client
from azure.cosmos.exceptions import CosmosResourceNotFoundError, CosmosHttpResponseError

class DupeChecker:

    def __init__(
            self,
            invoice_bytes: bytes,
            tenant_id: str,
            user_id: str,
    ) -> None:
        self.invoice_bytes = invoice_bytes
        self.tenant_id = tenant_id
        self.user_id = user_id
        self._bk: Optional[str] = None
        self.is_dupe = False
        self.is_byte_dupe = False
        self.is_bizkey_dupe = False
        self.invoice_id: Optional[str] = None
        self.hashed_bytes: None
        self.byte_id = None

    @property
    def bk(self) -> str | None:
        return self._bk

    @bk.setter
    def bk(self, bizkey: str) -> None:
        if bizkey and (self._bk is None or self._bk == bizkey):
            self._bk = bizkey
        else:
            raise ValueError("Could not set 'bizkey'.")
    
    @staticmethod
    def _get_db_client():
        return get_db_client().get_container_client("Invoices")
    
    def check_dupe(self) -> bool:
        try:
            return self._is_dupe()
        except ValueError as e:
            logging.warning("%s", e)
            return self.is_dupe

    def _is_dupe(self) -> bool:
        db_client = self._get_db_client()
        self.hashed_bytes = hashlib.sha256(self.invoice_bytes).hexdigest()
        self.byte_id = f"h|{self.hashed_bytes}"
        
        ## Check byte against possibly existing byte marker
        try:
            db_client.read_item(
                item= self.byte_id,
                partition_key= [self.tenant_id, self.user_id]  
            )
            self.is_dupe = True
            self.is_byte_dupe = True
        except CosmosHttpResponseError as e:
            if e.status_code != 404:
                raise
            
        if not self._bk:
            raise ValueError("Business key must first be set on the instance object")

        ## Check BK marker
        try:
            resp = db_client.read_item(
                item= self._bk,
                partition_key= [self.tenant_id, self.user_id] 
            )
            self.invoice_id = resp.get("invoice_id")
            self.is_dupe = True
            self.is_bizkey_dupe = True
        except CosmosHttpResponseError as e:
            if e.status_code != 404:
                raise
        
        return self.is_dupe


