from kivy.uix import boxlayout
from kivymd.uix import tab
from kivy.lang.builder import Builder
from . import ConfigWidget, ConnectionWidget, CollectorWidget, SubscriberWidget
import os

root_dir = os.path.split(__file__)[0]
Builder.load_file(os.path.join(root_dir, 'ConnectionWidget.kv'))
Builder.load_file(os.path.join(root_dir, 'SubscriberWidget.kv'))
Builder.load_file(os.path.join(root_dir, 'ConfigWidget.kv'))
Builder.load_file(os.path.join(root_dir, 'CollectorWidget.kv'))


class MainWidget(boxlayout.BoxLayout):

    pass


class TabWidget(tab.MDTabs):

    pass
