# -*- coding: utf-8 -*-
import unittest

from tredis import crc16


class CRC16TestCase(unittest.TestCase):

    def test_for_expected_values(self):
        for offset, (value, expectation) in enumerate([
                (b'123456789', 0x31c3),
                (b'Tornado is a Python web framework and asynchronous '
                 b'networking library, originally developed at FriendFeed.',
                 0x5a2a),
                (b'\xe2\x9c\x88', 0x8357)]):
            result = crc16.crc16(value)
            self.assertEqual(
                result, expectation,
                'Offset {} did not match (0x{:x} != 0x{:x})'.format(
                    offset, result, expectation))
