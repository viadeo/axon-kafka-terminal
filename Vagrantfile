Vagrant.configure("2") do |config|
  config.vm.box_url = 'http://files.vagrantup.com/precise64.box'
  config.vm.box = "precise64"

  config.vm.network "private_network", ip: "192.168.5.30", netmask: "255.255.255.0", drop_nat_interface_default_route: true
  config.vm.provider "virtualbox" do |v|
    v.memory = 2048
  end
  
  # Override auto-mount of '.' to '/home/vagrant/src'
  config.vm.synced_folder ".", "/vagrant", disabled: true
  config.vm.synced_folder ".", "/home/vagrant/src"

  config.vm.network "forwarded_port", guest: 9092, host: 9092
  config.vm.network "forwarded_port", guest: 2181, host: 2181

  config.vm.provision "shell", path: "vagrant/setup.sh"

end
