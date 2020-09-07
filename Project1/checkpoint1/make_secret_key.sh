# Check Arguments
if [ "$#" -ne 2 ]; then
    echo "Usage ./make_secret_key.sh <AWS_Instance_Public_DNS> <746_Key_file>"
    echo "example: ./make_secret_key.sh ec2-54-166-155-73.compute-1.amazonaws.com 746-student.pem"
    exit
fi

# Read arguments 
PublicDNS=$1
ClassKeyFile=$2

# Delete the old secret key files
rm -f secret_key secret_key.pub

# Crate new key
ssh-keygen -t rsa -N "" -f secret_key 

# Upload the new public key
echo "Updating key used on your AWS instance. Please be patient, this may take several seconds..."
cat secret_key.pub | ssh -i $ClassKeyFile  student@$PublicDNS 'cat > .ssh/authorized_keys'

# Configure public key permissions 
chmod 400 secret_key.pub

echo "Try: ssh -i secret_key student@$PublicDNS"
