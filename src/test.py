import aws_utils

aws_utils.get_pg_conn('master', 'localhost', 5439, "master", "AriLove0503", True, 10)

# **{config_constants.KEEPALIVE_INTERVAL_SECONDS: 5,
#                         config_constants.KEEPALIVE_AFTER_IDLE_SECONDS: 10}
