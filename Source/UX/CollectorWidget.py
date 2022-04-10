from kivy.uix import stacklayout
from kivymd.uix import tab


class CollectorWidget(tab.MDTabsBase, stacklayout.StackLayout):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)