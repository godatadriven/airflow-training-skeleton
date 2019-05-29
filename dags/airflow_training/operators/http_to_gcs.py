from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class HttpToGcsOperator(BaseOperator):
    """
    Calls an endpoint on an HTTP system to execute an action

    """

    @apply_defaults
    def __init__(
        self,
        *args,
        **kwargs
    ):
        super(HttpToGcsOperator, self).__init__(*args, **kwargs)


    def execute(self, context):
    	pass