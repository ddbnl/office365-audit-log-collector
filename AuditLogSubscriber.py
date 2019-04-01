"""
Taken from Microsoft sample script.
"""
# Standard libs
import sys
import logging
from collections import OrderedDict
# Internal libs
import ApiConnection


class AuditLogSubscriber(ApiConnection.ApiConnection):

    def get_info(self, question):
        """
        Args:
            question (str): question to ask user for input

        Returns:
            string of user input
        """
        while True:
            value = input(question)
            if value == '':
                continue
            else:
                return value

    def get_sub_status(self):

        status = self.make_api_request(url='subscriptions/list', append_url=True)
        return status.json()

    def set_sub_status(self, ctype_stat):
        """
        Args:
            ctype_stat (tuple): content type, status (enabled | disabled)

        Returns:
            dict
        """
        if ctype_stat[1] == 'enabled':
            action = 'stop'
        elif ctype_stat[1] == 'disabled':
            action = 'start'
        else:
            return
        status = self.make_api_request(url='subscriptions/{0}?contentType={1}'.format(action, ctype_stat[0]),
                                       append_url=True, get=False)

    def interactive(self):

        print('=' * 60)
        print('This script will enable or disable Office 365 subscriptions.')
        print('=' * 60)
        print('Please enter the required data.\n')

        print(('The Tenant ID is listed under Azure Active Directory | '
                'Properties and labeled "Directory ID".\nExample: '
                'cb6997bf-4029-455f-9f7a-e76fee8881da\n'))
        self.tenant_id = self.get_info('Enter Tenant ID: ')

        print(('\nThe Client Key is available after app registration and labeled "Application ID"'
                'App Registrations | <ESM App Name> | Application ID'
                '\nExample: '
                '553dd2ba-251b-47d5-893d-2f7ab26adf19\n'))
        self.client_key = self.get_info('Enter Client Key: ')

        print(('\nThe Secret Key is accessible only one time after the App has been registered:'
                '\nExample: '
                'D8perHbL9gAqx4vx5YbuffCDsvz2Pbdswey72FYRDNk=\n'))
        self.secret_key = self.get_info("Enter Secret Key: ")

        c = OrderedDict()
        while True:
            c['Audit.AzureActiveDirectory'] = 'disabled'
            c['Audit.Exchange'] = 'disabled'
            c['Audit.General'] = 'disabled'
            c['Audit.SharePoint'] = 'disabled'
            c['DLP.All'] ='disabled'

            status = self.get_sub_status()
            if status != '':
                try:
                    for s in status:
                        c[s['contentType']] = s['status']
                except (KeyError, TypeError):
                    print('Error: ', status['error']['message'])
                    sys.exit(1)

            print('\nEnter 1-5 to enable/disable subscriptions or 0 to exit')
            for idx, (c_type, status) in enumerate(c.items(), 1):
                print('{}. {}: {}'.format(idx, c_type, status))

            try:
                choice = int(self.get_info('Enter 0-5: '))
            except ValueError:
                continue
            menu = list(c.items())
            if 1 <= choice <= 5:
                self.set_sub_status(menu[choice - 1])
                continue
            elif choice == 6:
                continue
            elif choice == 0:
                break
            else:
                continue


if __name__ == "__main__":
    try:
        subscriber = AuditLogSubscriber()
        subscriber.interactive()
    except KeyboardInterrupt:
        logging.warning("Control-C Pressed, stopping...")
        sys.exit()
