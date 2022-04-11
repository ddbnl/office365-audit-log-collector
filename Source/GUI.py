import AuditLogCollector, AuditLogSubscriber
from kivymd.app import MDApp
from kivy.lang.builder import Builder
from kivy.config import Config
from kivy.clock import Clock
from kivy.properties import StringProperty
import os
import json
import time
import logging
import threading
import datetime
Config.set('graphics', 'resizable', False)
Config.set('graphics', 'width', '450')
Config.set('graphics', 'height', '600')
Config.write()


class GUI(MDApp):

    tenant_id = StringProperty()
    client_key = StringProperty()
    secret_key = StringProperty()
    publisher_id = StringProperty()

    def __init__(self, tenant_id="", client_key="", secret_key="", publisher_id="", **kwargs):

        self.title = "Audit log collector"
        super().__init__(**kwargs)
        self.root_widget = None
        self.publisher_id = publisher_id
        self.tenant_id = tenant_id
        self.client_key = client_key
        self.secret_key = secret_key
        self.subscriber = AuditLogSubscriber.AuditLogSubscriber()
        self.collector = AuditLogCollector.AuditLogCollector()
        self.successfully_finished = None
        self.running_continuously = False
        self.last_run_start = None
        self.run_thread = None

    def track_continuous(self, *args):

        if not self.running_continuously:
            return
        elif self.run_thread and self.run_thread.is_alive():
            time.sleep(1)
            return Clock.schedule_once(self.track_continuous)

        target_time = self.last_run_start + datetime.timedelta(
                hours=self.root_widget.ids.tab_widget.ids.config_widget.ids.run_time_slider.value)
        if datetime.datetime.now() >= target_time:
            self.run_collector()
            Clock.schedule_once(self.track_run)
        else:
            time_until_next = str(target_time - datetime.datetime.now()).split('.')[0]
            self.root_widget.ids.tab_widget.ids.collector_widget.ids.next_run_label.text = time_until_next
        time.sleep(1)
        return Clock.schedule_once(self.track_continuous)

    def track_run(self, *args):

        prefix = self.root_widget.ids.tab_widget
        if self.run_thread.is_alive():
            prefix.ids.collector_widget.ids.status_label.text = "Status: Running"
            self._update_run_statistics()
            time.sleep(0.5)
            Clock.schedule_once(self.track_run)
        else:
            if self.successfully_finished is True:
                prefix.ids.collector_widget.ids.status_label.text = "Status: Finished"
            else:
                prefix.ids.collector_widget.ids.status_label.text = "Error: {}".format(self.successfully_finished)
            self._update_run_statistics()
            prefix.ids.collector_widget.ids.run_time.text = \
                str(datetime.datetime.now() - self.collector.run_started).split(".")[0]
            if not self.running_continuously:
                prefix.ids.collector_widget.ids.run_once_button.disabled = False
                prefix.ids.collector_widget.ids.run_continuous_button.disabled = False
                prefix.ids.collector_widget.ids.run_continuous_button.text = 'Run continuously'

    def _update_run_statistics(self):

        prefix = self.root_widget.ids.tab_widget
        prefix.ids.collector_widget.ids.run_time.text = str(datetime.datetime.now() - self.collector.run_started)
        prefix.ids.collector_widget.ids.retrieved_label.text = str(self.collector.logs_retrieved)
        prefix.ids.collector_widget.ids.azure_sent_label.text = str(
            self.collector._azure_oms_interface.successfully_sent)
        prefix.ids.collector_widget.ids.azure_error_label.text = str(
            self.collector._azure_oms_interface.unsuccessfully_sent)
        prefix.ids.collector_widget.ids.graylog_sent_label.text = str(
            self.collector._graylog_interface.successfully_sent)
        prefix.ids.collector_widget.ids.graylog_error_label.text = str(
            self.collector._graylog_interface.unsuccessfully_sent)

    def run_once(self):

        self.run_thread = threading.Thread(target=self.run_collector, daemon=True)
        self.run_thread.start()
        Clock.schedule_once(self.track_run)

    def run_continuous(self):

        if not self.running_continuously:
            self.running_continuously = True
            self.run_thread = threading.Thread(target=self.run_collector, daemon=True)
            self.run_thread.start()
            Clock.schedule_once(self.track_continuous)
            Clock.schedule_once(self.track_run)
        else:
            self.running_continuously = False
            self.root_widget.ids.tab_widget.ids.collector_widget.ids.next_run_label.text = "-"
            self.root_widget.ids.tab_widget.ids.collector_widget.ids.run_continuous_button.text = \
                'Run continuously'

    def run_collector(self):

        self._prepare_to_run()
        self.last_run_start = datetime.datetime.now()
        self.root_widget.ids.tab_widget.ids.collector_widget.ids.next_run_label.text = "-"
        try:
            self.collector.run_once()
            self.successfully_finished = True
        except Exception as e:
            self.successfully_finished = e

    def _prepare_to_run(self):

        prefix = self.root_widget.ids.tab_widget
        prefix.ids.collector_widget.ids.run_once_button.disabled = True
        prefix.ids.collector_widget.ids.run_continuous_button.disabled = True
        if self.running_continuously:
            prefix.ids.collector_widget.ids.run_continuous_button.text = 'Stop running continuously'
        self.collector.content_types = prefix.ids.subscriber_widget.enabled_content_types
        self.collector.resume = prefix.ids.config_widget.ids.resume_switch.active
        fallback_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            hours=prefix.ids.config_widget.ids.collect_time_slider.value)
        self.collector._fallback_time = fallback_time
        self.collector.file_output = prefix.ids.config_widget.ids.file_output_switch.active
        self.collector.output_path = prefix.ids.config_widget.ids.file_output_path.text
        self.collector.azure_oms_output = prefix.ids.config_widget.ids.oms_output_switch.active
        self.collector.graylog_output = prefix.ids.config_widget.ids.graylog_output_switch.active
        self.collector._azure_oms_interface.workspace_id = prefix.ids.config_widget.ids.oms_id.text
        self.collector._azure_oms_interface.shared_key = prefix.ids.config_widget.ids.oms_key.text
        self.collector._graylog_interface.gl_address = prefix.ids.config_widget.ids.graylog_ip.text
        self.collector._graylog_interface.gl_port = prefix.ids.config_widget.ids.graylog_port.text
        if prefix.ids.config_widget.ids.log_switch.active:
            logging.basicConfig(filemode='w', filename='logs.txt', level=logging.DEBUG)

    @property
    def guid_example(self):

        return "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"

    def build(self):

        self.theme_cls.theme_style = "Dark"
        from UX import MainWidget
        Builder.load_file(os.path.join(os.path.split(__file__)[0], 'UX/MainWidget.kv'))
        self.root_widget = MainWidget.MainWidget()
        prefix = self.root_widget.ids.tab_widget
        prefix.ids.config_widget.ids.clear_known_content.disabled = not os.path.exists('known_content')
        prefix.ids.config_widget.ids.clear_last_run_times.disabled = not os.path.exists('last_run_times')
        self.load_settings()
        return self.root_widget

    def login(self, tenant_id, client_key, secret_key, publisher_id):

        if self.collector._headers:
            return self.logout()
        self.root_widget.ids.tab_widget.ids.connection_widget.ids.login_button.disabled = True
        self.tenant_id, self.client_key, self.secret_key, self.publisher_id = \
            tenant_id, client_key, secret_key, publisher_id
        self.subscriber.tenant_id = tenant_id
        self.subscriber.client_key = client_key
        self.subscriber.secret_key = secret_key
        self.subscriber.publisher_id = publisher_id
        if not tenant_id or not client_key or not secret_key:
            self.root_widget.connection_widget.ids.status_label.text = \
                "[color=#ff0000]Error logging in: provide tenant ID, client key and secret key. Find them in your " \
                "Azure AD app registration.[/color]"
            self.root_widget.ids.tab_widget.ids.connection_widget.ids.login_button.disabled = False
            return
        try:
            self.subscriber.login()
            self.root_widget.ids.status_label.text = "[color=#00ff00]Connected![/color]"
        except Exception as e:
            self.root_widget.ids.status_label.text = "[color=#ff0000]Error logging in: {}[/color]".format(e)
            self.root_widget.ids.tab_widget.ids.connection_widget.ids.login_button.disabled = False
            return
        login_headers = self.subscriber.headers
        self.collector._headers = login_headers

        self.root_widget.ids.tab_widget.ids.subscriber_widget.activate_switches()
        self.root_widget.ids.tab_widget.ids.subscriber_widget.set_switches()
        self.root_widget.ids.tab_widget.ids.collector_widget.ids.run_once_button.disabled = False
        self.root_widget.ids.tab_widget.ids.collector_widget.ids.run_continuous_button.disabled = False
        self.root_widget.ids.tab_widget.ids.connection_widget.ids.login_button.text = 'Disconnect'
        self.root_widget.ids.tab_widget.ids.connection_widget.ids.login_button.disabled = False

    def logout(self):

        self.collector._headers = None
        self.subscriber._headers = None
        self.root_widget.ids.status_label.text = "[color=#ffff00]Not logged in.[/color]"
        self.root_widget.ids.tab_widget.ids.subscriber_widget.deactivate_switches(reset_value=True)
        self.root_widget.ids.tab_widget.ids.collector_widget.ids.run_once_button.disabled = True
        self.root_widget.ids.tab_widget.ids.collector_widget.ids.run_continuous_button.disabled = True
        self.root_widget.ids.tab_widget.ids.connection_widget.ids.login_button.text = 'Connect'
        self.root_widget.ids.tab_widget.ids.connection_widget.ids.login_button.disabled = False

    def save_settings(self):

        prefix = self.root_widget.ids.tab_widget
        settings = dict()
        settings['tenant_id'] = self.tenant_id
        settings['client_key'] = self.client_key
        settings['include_secret_key'] = prefix.ids.config_widget.ids.include_secret_key_switch.active
        if prefix.ids.config_widget.ids.include_secret_key_switch.active:
            settings['secret_key'] = self.secret_key
        settings['publisher_id'] = self.publisher_id
        settings['resume'] = prefix.ids.config_widget.ids.resume_switch.active
        settings['run_time'] = prefix.ids.config_widget.ids.run_time_slider.value
        settings['fallback_time'] = prefix.ids.config_widget.ids.collect_time_slider.value
        settings['file_output'] = prefix.ids.config_widget.ids.file_output_switch.active
        settings['output_path'] = prefix.ids.config_widget.ids.file_output_path.text
        settings['azure_oms_output'] = prefix.ids.config_widget.ids.oms_output_switch.active
        settings['graylog_output'] = prefix.ids.config_widget.ids.graylog_output_switch.active
        settings['oms_workspace_id'] = prefix.ids.config_widget.ids.oms_id.text
        settings['oms_shared_key'] = prefix.ids.config_widget.ids.oms_key.text
        settings['gl_address'] = prefix.ids.config_widget.ids.graylog_ip.text
        settings['gl_port'] = prefix.ids.config_widget.ids.graylog_port.text
        settings['debug_logging'] = prefix.ids.config_widget.ids.log_switch.active
        with open('gui_settings.json', 'w') as ofile:
            json.dump(settings, ofile)

    def load_settings(self):

        if not os.path.exists('gui_settings.json'):
            return
        with open('gui_settings.json', 'r') as ofile:
            settings = json.load(ofile)
        prefix = self.root_widget.ids.tab_widget
        self.tenant_id = settings['tenant_id']
        self.client_key = settings['client_key']
        prefix.ids.config_widget.ids.include_secret_key_switch.active = settings['include_secret_key']
        if prefix.ids.config_widget.ids.include_secret_key_switch.active:
            self.secret_key = settings['secret_key']
        self.publisher_id = settings['publisher_id']
        prefix.ids.config_widget.ids.resume_switch.active = settings['resume']
        prefix.ids.config_widget.ids.run_time_slider.value = settings['run_time']
        prefix.ids.config_widget.ids.collect_time_slider.value = settings['fallback_time']
        prefix.ids.config_widget.ids.file_output_switch.active = settings['file_output']
        prefix.ids.config_widget.ids.file_output_path.text = settings['output_path']
        prefix.ids.config_widget.ids.oms_output_switch.active = settings['azure_oms_output']
        prefix.ids.config_widget.ids.graylog_output_switch.active = settings['graylog_output']
        prefix.ids.config_widget.ids.oms_id.text = settings['oms_workspace_id']
        prefix.ids.config_widget.ids.oms_key.text = settings['oms_shared_key']
        prefix.ids.config_widget.ids.graylog_ip.text = settings['gl_address']
        prefix.ids.config_widget.ids.graylog_port.text = settings['gl_port']
        prefix.ids.config_widget.ids.log_switch.active = settings['debug_logging']

    def clear_known_content(self):

        self.root_widget.ids.tab_widget.ids.config_widget.ids.clear_known_content.disabled = True
        if os.path.exists('known_content'):
            os.remove('known_content')

    def clear_last_run_times(self):

        self.root_widget.ids.tab_widget.ids.config_widget.ids.clear_last_run_times.disabled = True
        if os.path.exists('last_run_times'):
            os.remove('last_run_times')


if __name__ == '__main__':

    gui = GUI()
    gui.run()
