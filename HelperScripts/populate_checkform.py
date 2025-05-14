from pypdf import PdfReader,PdfWriter
from pathlib import Path

def fillout_form(invoice_fields: dict, table_fields: list, invoice: str) -> None:
    rbFile = Path("..") / "Invoices" / "check_rqf_template_acroform_1.pdf"
    wbFile = Path("..") / "Invoices" / "Check_Request_Forms" / f"{invoice}_check_request.pdf"

    reader = PdfReader(rbFile)
    writer = PdfWriter(clone_from=reader)

    
    ##We need to clean up the invoice_fields dict into a payload so that our check requests do not crash.
    ##Expected fields for check_request_form
    #InvoiceDate
    #InvoiceId -> if missing, CustomerId
    #DueDate
    #VendorName
    #InvoiceTotal

    field_check = False
    while not (field_check):
        for page in writer.pages:
            try:
                writer.update_page_form_field_values(
                    page=page,
                    fields={
                        "InvoiceDate": f"{invoice_fields["InvoiceDate"]}",
                        "InvoiceId": f"{invoice_fields["InvoiceId"]}",
                        "DueDate": f"{invoice_fields["DueDate"]}",
                        "VendorName": f"{invoice_fields["VendorName"]}",
                        "InvoiceTotal": f"{invoice_fields["InvoiceTotal"]}"}
                )
                i = -1
                for item in table_fields:
                    i = i + 1
                    writer.update_page_form_field_values(
                        page=page,
                        fields={
                            f"Description {i}": f"{item["Description"]} for an amount of ${item["Amount"]}"
                        }
                    )

                field_check = True
            except KeyError as k:
                missing_attribute = str(k.args[0])
                missing_attribute.replace("\'","\"")
                print(missing_attribute)
                invoice_fields[missing_attribute] = "Not Found"
                for e, i in invoice_fields.items():
                    if e == missing_attribute:
                        print(f"{e}: {i}")

    with open(wbFile, "wb") as out:
        writer.write(out)