import unittest
from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark import SparkContext

from c4_dataset_script.Arabic.remove_duplicate_text import remove_duplicate_text


# from c4_dataset_script.massivetext_utils import docs_dedup


class Test(TestCase):

    @classmethod
    def setUpClass(cls):
        # Set up a SparkContext and SparkSession
        cls.spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
        cls.sc = cls.spark.sparkContext

    @classmethod
    def tearDownClass(cls):
        # Stop the SparkContext
        cls.spark.stop()

    def test_duplicates_sentences(self):
        # Create a sample DataFrame with no duplicates

        input_docs = [
            {"url": "url1", "text": """"الزلابية من الحلويات الشهية و الذيذة التى يحبونها الأطفال والصغار.
                 لأنها هشه وجميلة والبعض يفضلون أكلها باردة وآخرين يفضلونها ساخنة مع مشروبك المفضل.
                اليوم سنتعرف على طريقة إعداد الزلابية فى مطبخك بطريقة ناجحة وبسيطة.
                إخلطى الدقيق مع الملح والنشا والخميرة والسكر والفانيليا، ثمّ أضيفى مقدار من الماء، وقلبى الخليط جيّدًا حتى تصبح لدينا عجينة سائلة.
                 تُترك العجينة حتى تختمر، ثم تُقلّب، وفى الوقت ذاته تُبلّل ملعقة صغيرة بالزيت، ثم تُشكّل بها الزلابية، وتقلى فى زيت متوسط السخونة. 
                تخرج الزلابية من الزيت عندما يصبح لونها ذهبيًّا فاتحًا، وتُترك قليلاً، ثم توضع ثانية فى الزيت حتى تتحمّر جيّدًا وتصبح مقرمشة، وبعدها توضع الزلابية فى الشربات ثم تُصفّى وتُقدّم."""
             },
            {"url": "url2", "text": """"الزلابية من الحلويات الشهية و الذيذة.
                 لأنها هشه وجميلة والبعض يفضلون أكلها باردة وآخرين يفضلونها ساخنة مع مشروبك المفضل.
                اليوم سنتعرف على طريقة إعداد الزلابية فى مطبخك بطريقة ناجحة وبسيطة.
                إخلطى الدقيق مع الملح والنشا والخميرة والسكر والفانيليا، ثمّ أضيفى مقدار من الماء، وقلبى الخليط جيّدًا.
                 تُترك العجينة حتى تختمر، ثم تُقلّب، وفى الوقت ذاته تُبلّل ملعقة صغيرة بالزيت، ثم تُشكّل بها الزلابية،. 
                تخرج الزلابية من الزيت عندما يصبح لونها ذهبيًّا فاتحًا، وتُترك قليلاً، ثم توضع ثانية فى الزيت حتى تتحمّر جيّدًا"""
             },
        ]
        input_rdd = self.sc.parallelize(input_docs)

        # Call the function
        deduplicated_items, removed_lines = remove_duplicate_text(input_rdd, 1)
        deduplicated_items_list = deduplicated_items.collect()
        removed_lines_list = removed_lines.collect()

        self.assertEqual(len(deduplicated_items_list), 2)
        self.assertEqual(len(removed_lines_list), 2)
        expected_removed_lines_list = [{'url1',
                                        '                 لأنها هشه وجميلة والبعض يفضلون أكلها باردة وآخرين يفضلونها ساخنة مع مشروبك المفضل.',
                                        2},
                                       {'url1',
                                        '                اليوم سنتعرف على طريقة إعداد الزلابية فى مطبخك بطريقة ناجحة وبسيطة.',
                                        2}
                                       ]
        self.assertEqual(expected_removed_lines_list, removed_lines_list)

    def test_basic_duplicates_simple_lines(self):
        # Test case with basic duplicate lines.
        input_docs = [
            {"url": "url1", "text": "line1\nline2\nline3\nline4\nline5"},
            {"url": "url2", "text": "line1\nline2\nline6\nline7\nline8\nline9\nline10\nline11"},
        ]
        input_rdd = self.sc.parallelize(input_docs)

        expected_result = [
            {"url": "url1", "text": "line1\nline2\nline3\nline4\nline5"},
            {"url": "url2", "text": "line6\nline7\nline8\nline9\nline10\nline11"},
        ]

        result, removed_lines = remove_duplicate_text(input_rdd)
        result_list = result.collect()
        removed_lines_list = removed_lines.collect()
        self.assertEqual(result_list, expected_result)
        self.assertEqual(removed_lines_list, [['url1', 'line1', 2], ['url1', 'line2', 2]])


if __name__ == '__main__':
    unittest.main()
