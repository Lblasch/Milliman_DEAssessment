import requests

with open('./DataEngineer_Exam/ccda_pre_signed_urls.csv', 'r') as urls:
    for presigned_url in urls:
        test = presigned_url.split('?')[0]
        file_name = test.split('/')[-1]
        file_name = file_name.strip()
        full_file_name = 'CCDA_Downloads/' + file_name

        # Send a GET request to the presigned URL
        response = requests.get(presigned_url.strip())

        # Check if the request was successful
        if response.status_code == 200:
            # Save the content to a file
            with open(full_file_name, 'wb') as f:
                f.write(response.content)
        else:
            print(f"Failed to download file: {response.status_code}")
