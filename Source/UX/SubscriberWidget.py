from kivy.uix import stacklayout
from kivymd.uix import tab
from kivy.app import App


class SubscriberWidget(tab.MDTabsBase, stacklayout.StackLayout):

    def __init__(self, **kwargs):

        super().__init__(**kwargs)

    @property
    def content_types(self):

        return ['Audit.AzureActiveDirectory', 'Audit.General', 'Audit.Exchange', 'Audit.SharePoint', 'DLP.All']

    @property
    def enabled_content_types(self):

        return [x for x in self.content_types if self.ids[x].active]

    def activate_switches(self):

        for content_type in self.content_types:
            self.ids[content_type].disabled = False

    def deactivate_switches(self, reset_value=False):

        for content_type in self.content_types:
            self.ids[content_type].disabled = True
            if reset_value:
                self.ids[content_type].active = False

    def on_switch_press(self, *args, name):

        App.get_running_app().subscriber.set_sub_status(content_type=name, action='start' if args[0].active else 'stop')
        self.set_switches()

    def set_switches(self):

        status = App.get_running_app().subscriber.get_sub_status()
        if status == '':
            return App.get_running_app().disconnect()
        disabled_content_types = self.content_types.copy()
        for s in status:
            if s['status'].lower() == 'enabled':
                disabled_content_types.remove(s['contentType'])
        for disabled_content_type in disabled_content_types:
            self.ids[disabled_content_type].active = False
        for enabled_content_type in [x for x in self.content_types if x not in disabled_content_types]:
            self.ids[enabled_content_type].active = True