from azure.ai.documentintelligence.models import AnalyzeResult


async def process_ai_results(results: dict[str, AnalyzeResult]):
    receipt_result = results["receiptAi"]
    invoice_result = results["invoiceAi"]
    # Process Results from AI
    invoice_fields = {}
    table_fields = []
    for doc in invoice_result.documents:
        # At this point we are inside of a document returned by results
        for key, value in doc.fields.items():
            # Here we go inside of the 'fields' key which contains our key-value pairs
            if "Items" in key:
                # Now we are inside of the Items key and are looking at its value
                # The value for the items key returns our table contents
                # Meaning that we are now looking into the table contents
                for row in value.value_array:
                    # The for-loop variable here gives us back each row of the table
                    table_items = {}
                    for attribute, content in row.value_object.items():
                        # Now we are iterating through the actual row (object)
                        # Where the 'attribute' variable represents our key
                        # and the 'content' variable represents its related value
                        for content_field, content_value in content.items():
                            # Now we are looking at the inner dictionary for each attribute
                            # 'content_field' represents the key, 'content_value' represents its value
                            if "content" in content_field:
                                # if we reach the 'content_field' 'content', lets create a new key-value pair
                                # for our dictionary that will hold only one table row
                                table_items[attribute]=content_value
                    table_fields.append(table_items)
            else:
                for field, dict_value in value.items():
                    if "content" in field:
                        invoice_fields[key] = dict_value
        for field, value in invoice_fields.items():
            print(f"{field}: {value}")
        for row in table_fields:
            print(row)

    try:
        merchant = receipt_result.documents[0].fields.get("MerchantName").value_string
    except AttributeError:
        merchant = "Not Found"
    try:
        total = receipt_result.documents[0].fields.get("Total").value_currency.amount
    except AttributeError:
        total = "Not Found"
    try:
        date = receipt_result.documents[0].fields.get("TransactionDate").value_date
    except AttributeError:
        date = "Not Found"

    if "VendorName" not in invoice_fields:
        invoice_fields["VendorName"] = merchant
    if "InvoiceTotal" not in invoice_fields:
        invoice_fields["InvoiceTotal"] = total
    if "InvoiceDate" not in invoice_fields:
        invoice_fields["InvoiceDate"] = date
    if "InvoiceId" not in invoice_fields:
            if "CustomerId" in invoice_fields:
                invoice_fields["InvoiceId"] = invoice_fields.get("CustomerId")
            elif "PurchaseOrder" in invoice_fields:
                invoice_fields["InvoiceId"] = invoice_fields.get("PurchaseOrder")
            elif "OrderNumber" in invoice_fields:
                invoice_fields["InvoiceId"] = invoice_fields.get("OrderNumber")
            else:
                invoice_fields["InvoiceId"] = "Not found"
    invoice_fields["VendorName"] = invoice_fields["VendorName"].replace("\n"," ").strip()

    return {"invoice_fields": invoice_fields, "table_fields": table_fields}