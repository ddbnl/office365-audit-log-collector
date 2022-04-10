from kivy.uix import boxlayout
from kivymd.uix import tab
from kivy.properties import ListProperty


class ConnectionWidget(tab.MDTabsBase, boxlayout.BoxLayout):

    status_color = ListProperty()

    def __init__(self, **kwargs):

        self.status_color = [1, 1, 0]
        super().__init__(**kwargs)