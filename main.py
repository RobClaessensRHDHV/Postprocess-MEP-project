"""This module contains the business logic of the function.

Use the automation_context module to wrap your function in an Autamate context helper
"""

import json
from pydantic import Field, SecretStr
from speckle_automate import (
    AutomateBase,
    AutomationContext,
    execute_automate_function,
)

import requests
import pandas as pd


class FunctionInputs(AutomateBase):
    """These are function author defined values.

    Automate will make sure to supply them matching the types specified here.
    Please use the pydantic model schema to define your inputs:
    https://docs.pydantic.dev/latest/usage/models/
    """
    # Username, Speckle token, API URL and token
    username: str = Field(title="Username")
    speckle_token: SecretStr = Field(title="Speckle token")
    api_url: SecretStr = Field(title="API URL")
    api_token: SecretStr = Field(title="API token")


def automate_function(
    automate_context: AutomationContext,
    function_inputs: FunctionInputs,
) -> None:
    """This is an example Speckle Automate function.

    Args:
        automate_context: A context helper object, that carries relevant information
            about the runtime context of this function.
            It gives access to the Speckle project data, that triggered this run.
            It also has convenience methods attach result data to the Speckle model.
        function_inputs: An instance object matching the defined schema.
    """
    # Retrieve relevant context
    ard = automate_context.automation_run_data
    pl = automate_context.automation_run_data.triggers[0].payload

    # Setup data for request
    data = {
        'datafusr_config': {
            'project_name': 'MEPPostprocessingProject',
            'source_url': f"{ard.speckle_server_url}/projects/{ard.project_id}/models/{pl.model_id}@{pl.version_id}",
            'speckle_token': function_inputs.speckle_token.get_secret_value(),
        }
    }

    # Setup headers for request
    headers = {
        'content-type': 'application/json',
        'enable-logging': 'False',
        'source-application': 'RoomBook',
        'return-type': 'tables',
        'username': function_inputs.username,
        'token': function_inputs.api_token.get_secret_value(),
    }

    # Implement a (temporary) workaround to pass headers as kwargs
    # The headers argument is only implemented from Django 4.2, which clashes with the MySQL db version < 8
    # headers = {f"HTTP_{k.replace('-', '_')}": v for k, v in (headers or {}).items()}

    # Print headers
    print('Headers:', headers)
    print('Headers type:', type(headers))
    for key, val in headers.items():
        print(f'{key}: {val}')

    # Set URL
    url = f"{function_inputs.api_url.get_secret_value()}/from_datafusr"

    # Print URL
    print('URL:', url)

    # Make a POST request to the MEP API, dumping and loading the data to avoid JSON serialization issues
    response = requests.post(url, json=json.loads(json.dumps(data)), headers=headers).json()

    # Try to print some output
    print('Response:', response)
    print('Response type:', type(response))
    for key, val in response.items():
        print(f'{key}: {val}')

    # Handle response
    if response.get('building_data'):

        try:

            # Create a dataframe from the API response
            building_data_df = pd.DataFrame(response['building_data'])

            # Convert to HTML
            building_data_html = building_data_df.to_html()

            # Store as HTML
            with open("building_data.html", "w") as fp:
                fp.write(building_data_html)

            # Attach the HTML table to the Speckle model
            automate_context.store_file_result("building_data_html")

            # Mark run as successful
            automate_context.mark_run_success("Building data table successfully generated!")

        except Exception as e:

            # Mark run as failed
            automate_context.mark_run_failed(f"Automation failed: {e}")

    else:

        # Mark run as failed
        automate_context.mark_run_failed("Automation failed: No building data could be retrieved!")


def automate_function_without_inputs(automate_context: AutomationContext) -> None:
    """A function example without inputs.

    If your function does not need any input variables,
     besides what the automation context provides,
     the inputs argument can be omitted.
    """
    pass


# make sure to call the function with the executor
if __name__ == "__main__":
    # NOTE: always pass in the automate function by its reference, do not invoke it!

    # pass in the function reference with the inputs schema to the executor
    execute_automate_function(automate_function, FunctionInputs)

    # if the function has no arguments, the executor can handle it like so
    # execute_automate_function(automate_function_without_inputs)
