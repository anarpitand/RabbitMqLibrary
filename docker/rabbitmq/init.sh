#!/bin/sh
set -eu

rabbitmqctl await_startup

rabbitmqctl add_vhost /quorum || true
rabbitmqctl set_permissions -p /quorum guest ".*" ".*" ".*"
