"""
Script to import survey data from SurveyMonkey.
"""

import json
import logging

from client import SurveyMonkeyApiClient, SurveyMonkeyDailyRateLimitConsumed

LOGGER = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s', datefmt='%m-%d %H:%M')
LOGGER.setLevel(logging.INFO)


# This schema can be generated based on https://api.surveymonkey.com/v3/surveys/<survey_id>/responses/bulk
SURVEY_SCHEMAS = [
    {
        "id": SURVEY_ID,  # add your survey id
        "title": "Programming Trends in 2023",
        # each key in below dict is a question id and can be taken from ["data"]["pages"][0]["questions]
        "question_transformers": {
            "116254887": "question1",
            "116259286": "question2",
            "116260170": "question3",
            "116261474": "question4",
            "116261685": "question5",
            "116261824": "question6",
        }
    }
]


class SurveyMonkeySurveyImporter:

    def __init__(self, surveys):
        self.surveys = surveys

    def start(self):
        """
        Main entery point.
        """

        for survey_schema in self.surveys:

            survey_id = survey_schema['id']
            last_successfull_export_timestamp = self.last_successfull_export_timestamp(survey_id)
            client = SurveyMonkeyApiClient(survey_id, last_successfull_export_timestamp)
            url = client.get_endpoint_url()

            while True:
                LOGGER.info(
                    "Fetching survey data. SurveyID: [%s], LastSuccessfullExportTimestamp: [%s], URL: [%s]",
                    survey_id,
                    last_successfull_export_timestamp,
                    url
                )

                try:
                    # fetch 100 responses at a time
                    survey_responses = client.fetch_survey_responses(url)
                    survey_responses = survey_responses.json()
                except SurveyMonkeyDailyRateLimitConsumed:
                    LOGGER.info("Consumed daily api call limit. Can not make more calls.")
                    return

                date_modified = None

                # for each response, clean and store the reponse in database
                for survey_response in survey_responses.get('data'):

                    cleaned_survey_response = self.clean_survey_response(survey_schema, survey_response)

                    self.store_survey_response(cleaned_survey_response)

                    LOGGER.info(
                        "Data exported for. Survey: [%s], SurveyResponseId: [%s]",
                        survey_id,
                        cleaned_survey_response["survey_response_id"],
                    )

                    date_modified = survey_response['date_modified']

                self.save_export_timestamp(survey_id, date_modified)

                LOGGER.info(
                    "Successfully exported data for survey [%s] till date [%s]",
                    survey_id,
                    date_modified
                )

                # fetch more data if next url is present else break and move to next survey
                url = survey_responses.get('links').get('next')
                if url is None:
                    break

            LOGGER.info("Completed survey export for ID: [%s]", survey_id)

        LOGGER.info("Command completed. Completed export for all surveys.")

    def clean_survey_response(self, survey_schema, survey_response):
        """
        Clean a complete single survey response.
        """
        cleaned = {}
        cleaned["survey_id"] = int(survey_schema["id"])
        cleaned["survey_response_id"] = int(survey_response["id"])

        question_transformers = survey_schema["question_transformers"]

        for page in survey_response["pages"]:
            for question in page["questions"]:

                field = question_transformers[question["id"]]

                learner_answers = []
                for answer in question["answers"]:
                    if "other_id" in answer:
                        raw_answer = answer["text"].strip()
                    else:
                        raw_answer = answer["simple_text"].strip()

                    learner_answers.append(raw_answer)

                # Perform field specific transformation
                cleaned[question['heading']] = getattr(self, f"transform_{field}")(learner_answers)

        return cleaned

    def transform_boolean_response(self, answer):
        """
        Transform a boolean response.
        """
        options = {"no": False, "yes": True}
        transformed = None

        if answer:
            transformed = options.get(answer[0].lower())

        return transformed

    def transform_single_choice_response(self, answer):
        """
        Transform a single choice text response.
        """
        transformed = None

        if answer:
            transformed = answer[0]

        return transformed

    def transform_multichoice_response(self, answer):
        """
        Transform a multi choice text response.
        """
        transformed = None

        if answer:
            transformed = answer

        return transformed

    def transform_rating_response(self, answer):
        """
        Transform for rating response.
        """
        transformed = None

        if answer:
            transformed = int(answer[0])

        return transformed

    def transform_question1(self, answer):
        return self.transform_multichoice_response(answer)

    def transform_question2(self, answer):
        return self.transform_single_choice_response(answer)

    def transform_question3(self, answer):
        return self.transform_multichoice_response(answer)

    def transform_question4(self, answer):
        return self.transform_rating_response(answer)

    def transform_question5(self, answer):
        return self.transform_single_choice_response(answer)

    def transform_question6(self, answer):
        return self.transform_boolean_response(answer)

    def store_survey_response(self, response):
        """
        Store cleaned and transform survey response to Database, CSV etc

        Currently we are just logging the response on console.
        """
        LOGGER.info(
            f"{json.dumps(response, indent=2)}"
        )

    def save_export_timestamp(self, survey_id, timestamp):
        """
        Store cleaned and transform survey response to Database.

        Currently we are just logging the survey_id and timestamp on console.
        """
        LOGGER.info(
            "Stored export timestamp. Survey: [%s], Timestamp: [%s]",
            survey_id,
            timestamp
        )

    def last_successfull_export_timestamp(self, survey_id):
        """
        Returns the timestamp of last successful export for a survey.

        Currently it just return `None`. Actual timestamp will be stored and accessed from a Database.
        """
        return None



importer = SurveyMonkeySurveyImporter(SURVEY_SCHEMAS)
importer.start()
