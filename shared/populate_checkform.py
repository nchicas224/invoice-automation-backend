from pypdf import PdfReader,PdfWriter
from pathlib import Path

def fillout_form(invoice_fields: dict, table_fields: list) -> PdfWriter:
    rbFile = Path() / "Templates" / "check_rqf_template_acroform_1.pdf"

    reader = PdfReader(rbFile)
    writer = PdfWriter(clone_from=reader)

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
                    amount = None
                    if item["Amount"]:
                        amount = f" for an amount of ${item["Amount"]}."
                    writer.update_page_form_field_values(
                        page=page,
                        fields={
                            f"Description {i}": f"{item["Description"]} {amount if amount else '.'}"
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
    return writer