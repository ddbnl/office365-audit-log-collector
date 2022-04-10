from kivymd.uix import stacklayout, tab
from kivy.uix import scrollview
import os


class ConfigWidget(scrollview.ScrollView, tab.MDTabsBase):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
