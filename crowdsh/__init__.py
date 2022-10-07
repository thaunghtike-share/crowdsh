
import datetime
import re

import boto3
import botocore.errorfactory
import markdown2

from airtable import Airtable
from bleach.linkifier import Linker
from bs4 import BeautifulSoup
from pynamodb.attributes import NumberAttribute, UnicodeAttribute
from pynamodb.models import DoesNotExist, Model
from yattag import Doc


class CrowdReputation(Model):
    class Meta:
        table_name = "OpsZero-CrowdReputation-prod"
        region = "us-west-2"

    worker = UnicodeAttribute(attr_name="pk", hash_key=True)
    approved = NumberAttribute(default=0)
    rejected = NumberAttribute(default=0)

    @classmethod
    def approve(cls, worker_id, amount):
        cls.increment(worker_id, "Approved", amount)

    @classmethod
    def reject(cls, worker_id, amount):
        cls.increment(worker_id, "Rejected", amount)

    @classmethod
    def increment(cls, worker_id, status, amount):
        kpi = None
        try:
            kpi = cls.get(worker_id)
        except DoesNotExist:
            kpi = cls(worker_id)

        if status == "Approved":
            kpi.approved += amount
        elif status == "Rejected":
            kpi.rejected += amount

        kpi.save()


LIVE_ENDPOINT = "https://mturk-requester.us-east-1.amazonaws.com"
SANDBOX_ENDPOINT = "https://mturk-requester-sandbox.us-east-1.amazonaws.com"


def set_blank(attrs, new=False):
    attrs[(None, "target")] = "_blank"
    return attrs


class Crowd:
    def __init__(self, config):
        self.config = config

        endpoint_url = LIVE_ENDPOINT
        if not self.config["Live"]:
            endpoint_url = SANDBOX_ENDPOINT

        session = boto3.Session(
            aws_access_key_id=self.config["MTurk"]["AwsAccessKeyId"],
            aws_secret_access_key=self.config["MTurk"]["AwsSecretAccessKey"],
        )
        self.client = session.client(
            service_name="mturk", region_name="us-east-1", endpoint_url=endpoint_url
        )
        self.table = Airtable(
            self.config["Airtable"]["AppKey"],
            self.config["Airtable"]["Table"],
            api_key=self.config["Airtable"]["ApiKey"],
        )
        self.records = self.table.get_all(view=self.config["Airtable"]["View"])

    # TODO: https://blog.mturk.com/tutorial-using-crowd-html-elements-b8990ec71057
    def questionXml(self, title, description, fields, row):
        linker = Linker(callbacks=[set_blank])
        doc, tag, text, line = Doc().ttl()

        with tag("html"):
            with tag("head"):
                doc.asis(
                    """
                    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
                    <script type='text/javascript' src='https://s3.amazonaws.com/mturk-public/externalHIT_v1.js'></script>
                    <script src="https://code.jquery.com/jquery-3.4.1.min.js" integrity="sha256-CSXorXvZcTkaix6Yvo6HppcZGetbYMGWSFlBw8HfCJo=" crossorigin="anonymous"></script>
                    <script>
                        $(document).ready(function() {
                            $("#mturk_form").bind("keypress", function(e) {
                                if (e.keyCode == 13) {
                                    return false;
                                }
                            });
                        });
                    </script>
                """
                )
                line(
                    "link",
                    "",
                    rel="stylesheet",
                    href="https://stackpath.bootstrapcdn.com/bootstrap/4.1.3/css/bootstrap.min.css",
                    integrity="sha384-MCw98/SFnGE8fJT3GXwEOngsV7Zt27NXFoaoApmYm81iuXoPkFOJwJ8ERdknLPMO",
                    crossorigin="anonymous",
                )

            with tag("body"):
                with tag("div", klass="container"):
                    with tag("div", klass="row"):
                        with tag("div", klass="col"):
                            line("h1", title)
                            with tag("div"):
                                doc.asis(markdown2.markdown(description))

                            with tag(
                                "form",
                                name="mturk_form",
                                method="post",
                                id="mturk_form",
                                action="https://www.mturk.com/mturk/externalSubmit",
                            ):
                                for field in fields:
                                    value = ""
                                    if field["Name"] in row:
                                        value = re.sub(
                                            r"[^\x00-\x7f]", r"", row[field["Name"]]
                                        )

                                    with tag("div", klass="form-group"):
                                        if field["Type"] != "Hidden":
                                            with tag("label"):
                                                line("strong", field["Name"])
                                            with tag(
                                                "small", klass="form-text text-muted"
                                            ):
                                                doc.asis(
                                                    markdown2.markdown(
                                                        field["Description"]
                                                    )
                                                )

                                        if field["Type"] == "Hidden":
                                            doc.stag(
                                                "input",
                                                type="hidden",
                                                name=field["Name"],
                                                value=value,
                                            )
                                        elif field["Type"] == "Image":
                                            doc.stag(
                                                "img", src=value, klass="img")
                                        elif field["Type"] == "Label":
                                            doc.asis(linker.linkify(value))
                                        elif field["Type"] == "LongText":
                                            with tag(
                                                "textarea",
                                                name=field["Name"],
                                                klass="form-control",
                                                cols="80",
                                                rows="3",
                                            ):
                                                text(value)
                                        if field["Type"] == "Checkbox":
                                            doc.stag(
                                                "input",
                                                type="checkbox",
                                                name=field["Name"],
                                                value="yes",
                                            )
                                        elif field["Type"] == "Radio":
                                            for v in field["Options"]:
                                                with tag("div", klass="form-check"):
                                                    doc.stag(
                                                        "input",
                                                        type="radio",
                                                        klass="form-check-input",
                                                        name=field["Name"],
                                                        value=v,
                                                    )
                                                    with tag(
                                                        "label",
                                                        klass="form-check-label",
                                                    ):
                                                        text(v)
                                        elif field["Type"] == "Select":
                                            with tag(
                                                "select",
                                                name=field["Name"],
                                                klass="form-control",
                                            ):
                                                for v in field["Options"]:
                                                    with tag("option", value=v):
                                                        text(v)
                                        elif field["Type"] == "ShortText":
                                            doc.stag(
                                                "input",
                                                type="text",
                                                klass="form-control",
                                                name=field["Name"],
                                                value=value,
                                            )

                                line(
                                    "p",
                                    "NOTE: All work is checked and if we find the work does not follow the instructions we will reject the work. If we find a consistent amount of bad work you will be automatically blocked from our work in the future.",
                                )
                                line("p", "Thank you for your good work!")

                                doc.stag(
                                    "input",
                                    type="hidden",
                                    value="",
                                    name="assignmentId",
                                    id="assignmentId",
                                )
                                doc.stag(
                                    "input",
                                    type="submit",
                                    id="submitButton",
                                    value="Submit",
                                    klass="btn btn-success",
                                )
                line("script", "turkSetAssignmentID();", language="Javascript")

        return f"""<HTMLQuestion xmlns='http://mechanicalturk.amazonaws.com/AWSMechanicalTurkDataSchemas/2011-11-11/HTMLQuestion.xsd'><HTMLContent><![CDATA[{doc.getvalue()}]]></HTMLContent><FrameHeight>0</FrameHeight></HTMLQuestion>"""

    def approve(self, hit_id):
        assignments = self.client.list_assignments_for_hit(
            HITId=hit_id, AssignmentStatuses=["Submitted", "Approved"]
        )
        worker_id = assignments["Assignments"][0]["WorkerId"]
        CrowdReputation.approve(f"MTurk:{worker_id}", 1)

    def reject(self, hit_id):
        try:
            assignments = self.client.list_assignments_for_hit(
                HITId=hit_id, AssignmentStatuses=["Submitted", "Approved"]
            )
            worker_id = assignments["Assignments"][0]["WorkerId"]
            assignment_id = assignments["Assignments"][0]["AssignmentId"]
            kpi_name = f"MTurk:{worker_id}"
            Reputation.reject(kpi_name, 1)
            self.block_bad_workers(kpi_name, worker_id, assignment_id)
        except:
            print("Can't find assignment")

    def block_bad_workers(self, kpi_name, worker_id, assignment_id):
        kpi = CrowdReputation.get(kpi_name)
        count = kpi.approved + kpi.rejected
        if count == 0:
            count = 1
        success_rate = (1.0 - (kpi.rejected / count)) * 100

        # Wait till the number of tasks is 10 before blocking.
        if count > 3 and success_rate < 75:
            print(f"Blocking Worker: {worker_id}")
            self.client.create_worker_block(
                WorkerId=worker_id, Reason="Bad Work Quality"
            )
            try:
                print("Rejecting Assignment")
                self.client.reject_assignment(
                    AssignmentId=assignment_id, RequesterFeedback="Bad Work Quality"
                )
            except:
                print("Error Rejecting Worker")

    def iter(self):
        i = 0
        num_records = len(self.records)
        for record in self.records:
            print(f"Record {i} / {num_records}")
            i += 1

            fields = record["fields"]
            updated_fields = {}

            if "DataStoryStatus" not in record["fields"]:  # Status: Empty
                yield record
            elif fields.get("DataStoryStatus") in ["Draft", "Working"]:
                yield record
                if fields.get("DataStoryHitID") in [None, ""]:
                    try:
                        resp = self.client.create_hit(
                            Reward=self.config["MTurk"]["Reward"],
                            Title=self.config["MTurk"]["Title"],
                            Keywords=self.config["MTurk"]["Keywords"],
                            Description=self.config["MTurk"]["Description"],
                            AutoApprovalDelayInSeconds=86_400,
                            AssignmentDurationInSeconds=600,
                            MaxAssignments=1,
                            LifetimeInSeconds=86400,
                            Question=self.questionXml(
                                self.config["MTurk"]["Title"],
                                self.config["MTurk"]["Description"],
                                self.config["Fields"],
                                fields,
                            ),
                        )

                        updated_fields["DataStoryHitID"] = resp["HIT"]["HITId"]
                        updated_fields["DataStoryStatus"] = "Working"

                    except botocore.exceptions.ClientError as e:
                        print(e)
                        if e.response["Error"]["Code"] == "ParameterValidationError":
                            updated_fields["DataStoryStatus"] = "Error"
                else:
                    print("Working Record")

                    assignmentsList = self.client.list_assignments_for_hit(
                        HITId=fields["DataStoryHitID"]
                    )
                    assignments = assignmentsList["Assignments"]

                    if len(assignments) > 0:
                        print("Here")
                        assignment = assignments[0]
                        soup = BeautifulSoup(
                            assignment["Answer"], "html.parser")

                        for answer in soup.select("answer"):
                            field_name = answer.select(
                                "questionidentifier")[0].string
                            field_answer = answer.select("freetext")[0].string

                            for config_field in self.config["Fields"]:
                                if config_field["Name"] == field_name:
                                    updated_fields[field_name] = field_answer

                        updated_fields["DataStoryStatus"] = "Finished"
                    else:
                        hit = self.client.get_hit(
                            HITId=fields["DataStoryHitID"])
                        if hit["HIT"]["HITStatus"] == "Disposed":
                            print("Remove Old HITs")
                            updated_fields["DataStoryHitID"] = ""
                            updated_fields["DataStoryStatus"] = None
                        else:
                            try:
                                print("Adding 18 hours of time")
                                expire_in_hours = datetime.datetime.now(
                                    datetime.timezone.utc
                                ) + datetime.timedelta(hours=18)
                                self.client.update_expiration_for_hit(
                                    HITId=fields["DataStoryHitID"],
                                    ExpireAt=expire_in_hours,
                                )
                            except botocore.exceptions.ClientError as e:
                                print(e)

                if len(updated_fields) > 0:
                    self.table.update(record["id"], updated_fields)
            elif record["fields"].get("DataStoryStatus") == "Finished":
                yield record
            elif record["fields"].get("DataStoryStatus") == "Manual":
                yield record
            elif record["fields"].get("DataStoryStatus") == "Approved":
                if "DataStoryHitID" in record["fields"]:
                    self.approve(record["fields"]["DataStoryHitID"])
                    self.table.update(
                        record["id"],
                        {
                            "DataStoryHitID": "",
                        },
                    )

                yield record
            elif record["fields"].get("DataStoryStatus") == "Rejected":
                if "DataStoryHitID" in record["fields"]:
                    self.reject(record["fields"]["DataStoryHitID"])

                data = {
                    "DataStoryHitID": "",
                    "DataStoryStatus": None,
                }
                self.table.update(
                    record["id"],
                    {
                        **data,
                        **{
                            field: ""
                            for field in [
                                field["Name"]
                                for field in self.config["Fields"]
                                if field["Type"] != "Label"
                            ]
                        },
                    },
                )

                yield record
            else:
                yield record

    def balance(self):
        return self.client.get_account_balance()["AvailableBalance"]
