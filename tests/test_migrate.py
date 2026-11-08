"""migrate_influx2vm.py must copy tags - sensor_uid included - as labels.

The script runs its migration at import time, so only the pure helpers are
extracted from its source and executed in isolation.
"""
import ast
import unittest

from helpers import REPO


def load_helpers():
    src = (REPO / 'migrate_influx2vm.py').read_text()
    tree = ast.parse(src)
    keep = [node for node in tree.body
            if (isinstance(node, ast.FunctionDef) and node.name in ('record_labels', 'with_port'))
            or (isinstance(node, ast.Assign)
                and any(getattr(t, 'id', None) == 'LABEL_KEYS' for t in node.targets))]
    ns = {}
    exec(compile(ast.Module(body=keep, type_ignores=[]), 'migrate_helpers', 'exec'), ns)
    return ns


class Labels(unittest.TestCase):

    def setUp(self):
        self.ns = load_helpers()

    def test_sensor_uid_is_a_label(self):
        self.assertIn('sensor_uid', self.ns['LABEL_KEYS'])
        values = {'_time': 'x', 'sensor_id': 12345.6789, 'sensor_name': 'Kitchen',
                  'sensor_uid': '12345.6789012345678901', 'temperature': 21.5}
        self.assertEqual(self.ns['record_labels'](values), {
            'sensor_id': '12345.6789', 'sensor_name': 'Kitchen',
            'sensor_uid': '12345.6789012345678901'})

    def test_points_without_sensor_uid(self):
        self.assertEqual(self.ns['record_labels'](
            {'sensor_id': '1.5', 'sensor_name': 'n', 'sensor_uid': None}),
            {'sensor_id': '1.5', 'sensor_name': 'n'})

    def test_port_is_appended_only_when_missing(self):
        with_port = self.ns['with_port']
        self.assertEqual(with_port('http://db', '8086'), 'http://db:8086')
        self.assertEqual(with_port('http://db:9999', '8086'), 'http://db:9999')


if __name__ == '__main__':
    unittest.main()
