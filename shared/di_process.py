import os
from azure.core.credentials import AzureKeyCredential
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeResult

def get_invoice_fields(stream: bytes):
    if not (stream):
        return "Stream was empty!"
    
    endpoint = os.environ["DI_ENDPOINT"]
    key = os.environ["DI_API_KEY"] ## ADD THESE TO AZURE ENV

    document_intelligence_client = DocumentIntelligenceClient(endpoint=endpoint, credential=AzureKeyCredential(key))
    try:
        pdf_bytes = stream
        invoice = document_intelligence_client.begin_analyze_document("prebuilt-invoice", pdf_bytes, locale="en-US")
        receipt = document_intelligence_client.begin_analyze_document("prebuilt-receipt", pdf_bytes, locale="en-US")
        try:
            invoice_results: AnalyzeResult = invoice.result()
            receipt_results: AnalyzeResult = receipt.result()
            results = {"invoiceAi": invoice_results, "receiptAi": receipt_results}
            return results
        except KeyboardInterrupt as k:
            return f"Document AI failed: {k}"
    except Exception as e:
        return f"Document AI failed: {e}"

# def main():
#     stream = "."
#     result = get_invoice_fields(stream)
#     print(result)

# if __name__ == "__main__":
#     main()