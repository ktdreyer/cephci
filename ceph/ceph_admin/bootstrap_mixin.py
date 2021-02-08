"""
Cephadm Bootstrap the ceph cluster
"""

import logging

from utility.utils import get_cephci_config

logger = logging.getLogger(__name__)


class BootstrapMixin:
    def bootstrap(self):
        """
        Bootstrap the ceph cluster with supported options

        Bootstrap involves,
          - Creates /etc/ceph directory with permissions
          - CLI creation with bootstrap options with custom/default image
          - Execution of bootstrap command
        """
        # copy ssh keys to other hosts
        self.cluster.setup_ssh_keys()

        # set tool download repository
        self.set_tool_repo()

        # install/download cephadm package on installer
        self.install_cephadm()

        # Create and set permission to ceph directory
        self.installer.exec_command(sudo=True, cmd="mkdir -p /etc/ceph")
        self.installer.exec_command(sudo=True, cmd="chmod 777 /etc/ceph")

        # Execute bootstrap with MON ip-address
        # Construct bootstrap command
        # 1) Skip default mon, mgr & crash specs
        # 2) Skip automatic dashboard provisioning

        cmd = ['cephadm', '-v']

        image = self.config.get('container_image')
        if not self.config.get("registry") and image:
            cmd += ['--image', image]

        cmd += ['bootstrap',
                '--orphan-initial-daemons',
                '--skip-monitoring-stack',
                '--mon-ip', self.installer.node.ip_address,
                ]

        cephci_config = get_cephci_config()
        if 'cdn_credentials' not in cephci_config:
                # cephadm installs a Prometheus image from the terms-based
                # registry at registry.redhat.io, and that requires Red Hat CDN
                # credentials.
                raise KeyError('cephadm requires cdn_credentials in ~/.cephci.yaml')
        cdn_cred = cephci_config['cdn_credentials']
        cmd += [
            '--registry-url', 'registry.redhat.io',
            '--registry-username', cdn_cred['username'],
            '--registry-password', cdn_cred['password'],
        ]

        cmd = ' '.join(cmd)

        out, err = self.installer.exec_command(
            sudo=True,
            cmd=cmd,
            timeout=1800,
            check_ec=True,
        )

        logger.info("Bootstrap output : %s", out.read().decode())
        logger.error("Bootstrap error: %s", err.read().decode())

        self.distribute_cephadm_gen_pub_key()
