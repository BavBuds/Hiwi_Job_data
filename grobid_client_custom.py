import httpx

class GrobidClient:
    def __init__(self, host="http://localhost:8070", timeout=300):
        self.host = host
        self.timeout = timeout

    def process(self, service, pdf_path, output):
        files = {'input': open(pdf_path, 'rb')}
        response = httpx.post(f"{self.host}/api/{service}", files=files, timeout=self.timeout)
        if response.status_code == 200:
            with open(output, 'wb') as f:
                f.write(response.content)
        else:
            response.raise_for_status()
