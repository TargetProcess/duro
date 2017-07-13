from raven import Client


def post_to_sentry():
    client = Client('')
    client.captureException()
