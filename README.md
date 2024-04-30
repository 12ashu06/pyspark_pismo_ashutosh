# Pyspark Assignment Pismo - Ashutosh

This project demonstrates a balance withdrawal processing using PySpark. The code consists of:

- Definition of schemas for balance and withdrawal tables.
- Creation of dummy data for balance and withdrawal tables.
- Joining the balance and withdrawal tables based on account ID and order.
- Processing withdrawals using a function that checks for available balance and updates it accordingly.
- Writing the result to an output file or repository.

The `process_withdrawals` function validates withdrawal amounts against available balances and updates the balance accordingly. The processed data is then written to a CSV file.

The code is structured and commented for clarity, making it easy to understand and modify as needed.


# Install Depenedency
- RUN pip install pyspark
- RUN pip install pytest


# RUN script.py file
- python script.py

# To RUN Pytest 
- RUN tests/pytest test.py