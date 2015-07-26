#ABSTRACT: An asynchronous library for InfluxDB time-series database
use strict;
use warnings;
package AnyEvent::InfluxDB;

use AnyEvent;
use AnyEvent::HTTP;
use URI;
use URI::QueryParam;
use JSON qw(decode_json);
use List::MoreUtils qw(zip);
use Moo;

has [qw( ssl_options username password on_request )] => (
    is => 'ro',
    predicate => 1,
);

has 'server' => (
    is => 'rw',
    default => 'http://localhost:8086',
);

has '_is_ssl' => (
    is => 'lazy',
);

has '_tls_ctx' => (
    is => 'lazy',
);

has '_server_uri' => (
    is => 'lazy',
);

=head1 SYNOPSIS

    use EV;
    use AnyEvent;
    use AnyEvent::Socket;
    use AnyEvent::Handle;
    use AnyEvent::InfluxDB;

    my $db = AnyEvent::InfluxDB->new(
        server => 'http://localhost:8086',
        username => 'admin',
        password => 'password',
    );

    my $hdl;
    tcp_server undef, 8888, sub {
        my ($fh, $host, $port) = @_;

        $hdl = AnyEvent::Handle->new(
            fh => $fh,
        );

        $hdl->push_read(
            line => sub {
                my (undef, $line) = @_;

                $db->write(
                    database => 'mydb',
                    data => $line,
                    on_success => sub { print "$line written\n"; },
                    on_error => sub { print "$line error: @_\n"; },
                );

                $hdl->on_drain(
                    sub {
                        $hdl->fh->close;
                        undef $hdl;
                    }
                );
            },
        );
    };

    EV::run;

=head1 DESCRIPTION

Asynchronous client library for InfluxDB time-series database v0.9.2 L<https://influxdb.com>.

=head1 METHODS

=head2 new

    my $db = AnyEvent::InfluxDB->new(
        server => 'http://localhost:8086',
        username => 'admin',
        password => 'password',
    );

Returns object representing given server C<server> connected using optionally
provided username C<username> and password C<password>.

Default value of C<server> is C<http://localhost:8086>.

If the server protocol is C<https> then by default no validation of remote
host certificate is performed. This can be changed by setting C<ssl_options>
parameter with any options accepted by L<AnyEvent::TLS>.

    my $db = AnyEvent::InfluxDB->new(
        server => 'https://localhost:8086',
        username => 'admin',
        password => 'password',
        ssl_options => {
            verify => 1,
            verify_peername => 'https',
            ca_file => '/path/to/cacert.pem',
        }
    );

As an debugging aid the C<on_request> code reference may also be provided. It will
be executed before each request with the method name, url and POST data if set.

    my $db = AnyEvent::InfluxDB->new(
        on_request => sub {
            my ($method, $url, $post_data) = @_;
            print "$method $url\n";
            print "$post_data\n" if $post_data;
        }
    );

=for Pod::Coverage has_on_request has_password has_ssl_options has_username on_request password server ssl_options username

=cut

sub _build__tls_ctx {
    my ($self) = @_;

    # no ca/hostname checks
    return 'low' unless $self->has_ssl_options;

    # create ctx
    require AnyEvent::TLS;
    return AnyEvent::TLS->new( %{ $self->ssl_options } );
}

sub _build__is_ssl {
    my ($self) = @_;

    return $self->server =~ /^https/;
}

sub _build__server_uri {
    my ($self) = @_;

    my $url = URI->new( $self->server, 'http' );

    if ( $self->has_username && $self->has_password ) {
        $url->query_param( 'u' => $self->username );
        $url->query_param( 'p' => $self->password );
    }

    return $url;
}

sub _make_url {
    my ($self, $path, $params) = @_;

    my $url = $self->_server_uri->clone;
    $url->path($path);

    while ( my ($k, $v) = each %$params ) {
        $url->query_param( $k => $v );
    }

    return $url;
}

sub _http_request {
    my $cb = pop;
    my ($self, $method, $url, $post_data) = @_;

    if ($self->has_on_request) {
        $self->on_request->($method, $url, $post_data);
    }

    my $guard;
    $guard = http_request
        $method => $url,
        (
            $post_data ? ( body => $post_data ) : ()
        ),
        (
            $self->_is_ssl ?
            (
                tls_ctx => $self->_tls_ctx
            ) : ()
        ),
        sub {
            $cb->(@_);
            undef $guard;
        };
};

=head2 Database Management

=head3 create_database

    $cv = AE::cv;
    $db->create_database(
        database => "mydb",
        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to create database: @_");
        }
    );
    $cv->recv;

Creates specified by C<database> argument database.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub create_database {
    my ($self, %args) = @_;

    my $url = $self->_make_url('/query', {
        q => 'CREATE DATABASE '. $args{database}
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' && $body eq '{"results":[{}]}' ) {
                $args{on_success}->();
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 drop_database

    $cv = AE::cv;
    $db->drop_database(
        database => "mydb",
        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to drop database: @_");
        }
    );
    $cv->recv;

Drops specified by C<database> argument database.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub drop_database {
    my ($self, %args) = @_;

    my $url = $self->_make_url('/query', {
        q => 'DROP DATABASE '. $args{database}
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' && $body eq '{"results":[{}]}' ) {
                $args{on_success}->();
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}
=head3 show_databases

    $cv = AE::cv;
    $db->show_databases(
        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to list databases: @_");
        }
    );
    my @db_names = $cv->recv;
    print "$_\n" for @db_names;

Returns list of known database names.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub show_databases {
    my ($self, %args) = @_;

    my $url = $self->_make_url('/query', {
        q => 'SHOW DATABASES'
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' ) {
                my $data = decode_json($body);
                my @names;
                eval {
                    @names = map { $_->[0] } @{ $data->{results}->[0]->{series}->[0]->{values} || [] };
                };
                $args{on_success}->(@names);
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head2 Retention Policy Management

=head3 create_retention_policy

    $cv = AE::cv;
    $db->create_retention_policy(
        name => 'last_day',
        database => 'mydb',
        duration => '1d',
        replication => 1,
        default => 0,

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to create retention policy: @_");
        }
    );
    $cv->recv;

Creates new retention policy named by C<name> on database C<database> with
duration C<duration> and replication factor C<replication>. If C<default> is
provided and true the created retention policy becomes the default one.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub create_retention_policy {
    my ($self, %args) = @_;

    my $q = 'CREATE RETENTION POLICY '. $args{name}
        .' ON '. $args{database}
        .' DURATION '. $args{duration}
        .' REPLICATION '. $args{replication};

    $q .= ' DEFAULT' if $args{default};

    my $url = $self->_make_url('/query', {
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' && $body eq '{"results":[{}]}' ) {
                $args{on_success}->();
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 alter_retention_policy

    $cv = AE::cv;
    $db->alter_retention_policy(
        name => 'last_day',
        database => 'mydb',

        duration => '1d',
        replication => 1,
        default => 0,

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to alter retention policy: @_");
        }
    );
    $cv->recv;

Modifies retention policy named by C<name> on database C<database>. At least one
of duration C<duration>, replication factor C<replication> or flag C<default>
must be set.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub alter_retention_policy {
    my ($self, %args) = @_;

    my $q = 'ALTER RETENTION POLICY '. $args{name}
        .' ON '. $args{database};

    $q .= ' DURATION '. $args{duration} if exists $args{duration};
    $q .= ' REPLICATION '. $args{replication} if exists $args{replication};;
    $q .= ' DEFAULT' if $args{default};

    my $url = $self->_make_url('/query', {
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' && $body eq '{"results":[{}]}' ) {
                $args{on_success}->();
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 show_retention_policies

    $cv = AE::cv;
    $db->show_retention_policies(
        database => 'mydb',

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to list retention policies: @_");
        }
    );
    my @retention_policies = $cv->recv;
    for my $rp ( @retention_policies ) {
        print "Name: $rp->{name}\n";
        print "Duration: $rp->{duration}\n";
        print "Replication factor: $rp->{replicaN}\n";
        print "Default?: $rp->{default}\n";
    }

Returns a list of hash references with keys C<name>, C<duration>, C<replicaN>
and C<default> for each replication policy defined on database C<database>.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub show_retention_policies {
    my ($self, %args) = @_;

    my $url = $self->_make_url('/query', {
        q => 'SHOW RETENTION POLICIES ON '. $args{database}
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' ) {
                my $data = decode_json($body);
                my $res = $data->{results}->[0]->{series}->[0];
                my $cols = $res->{columns};
                my $values = $res->{values};
                my @policies = (
                    map {
                        +{
                            zip(@$cols, @$_)
                        }
                    } @{ $values || [] }
                );
                $args{on_success}->(@policies);
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head2 User Management

=head3 create_user

    $cv = AE::cv;
    $db->create_user(
        username => 'jdoe',
        password => 'mypassword',
        all_privileges => 1,

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to create user: @_");
        }
    );
    $cv->recv;

Creates user with C<username> and C<password>. If flag C<all_privileges> is set
to true created user will be granted cluster administration privileges.

Note: C<password> will be automatically enclosed in single quotes.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub create_user {
    my ($self, %args) = @_;

    my $q = 'CREATE USER '. $args{username}
        .' WITH PASSWORD \''. $args{password} .'\'';

    $q .= ' WITH ALL PRIVILEGES' if $args{all_privileges};

    my $url = $self->_make_url('/query', {
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' && $body eq '{"results":[{}]}' ) {
                $args{on_success}->();
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 set_user_password

    $cv = AE::cv;
    $db->set_user_password(
        username => 'jdoe',
        password => 'otherpassword',

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to set password: @_");
        }
    );
    $cv->recv;

Sets password to C<password> for the user identified by C<username>.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub set_user_password {
    my ($self, %args) = @_;

    my $q = 'SET PASSWORD FOR '. $args{username}
        .' = \''. $args{password} .'\'';

    my $url = $self->_make_url('/query', {
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' && $body eq '{"results":[{}]}' ) {
                $args{on_success}->();
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 show_users

    $cv = AE::cv;
    $db->show_users(
        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to list users: @_");
        }
    );
    my @users = $cv->recv;
    for my $u ( @users ) {
        print "Name: $u->{user}\n";
        print "Admin?: $u->{admin}\n";
    }

Returns a list of hash references with keys C<user> and C<admin> for each
defined user.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub show_users {
    my ($self, %args) = @_;

    my $url = $self->_make_url('/query', {
        q => 'SHOW USERS'
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' ) {
                my $data = decode_json($body);
                my $res = $data->{results}->[0]->{series}->[0];
                my $cols = $res->{columns};
                my $values = $res->{values};
                my @users = (
                    map {
                        +{
                            zip(@$cols, @$_)
                        }
                    } @{ $values || [] }
                );
                $args{on_success}->(@users);
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 grant_privileges

    $cv = AE::cv;
    $db->grant_privileges(
        username => 'jdoe',

        # privileges at single database
        database => 'mydb',
        access => 'ALL',

        # or to grant cluster administration privileges
        all_privileges => 1,

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to grant privileges: @_");
        }
    );
    $cv->recv;

Grants to user C<username> access C<access> on database C<database>.
If flag C<all_privileges> is set it grants cluster administration privileges
instead.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub grant_privileges {
    my ($self, %args) = @_;

    my $q = 'GRANT ';

    if ( $args{all_privileges} ) {
        $q .= 'ALL PRIVILEGES';
    } else {
        $q .= $args{access} .' ON '. $args{database};
    }
    $q .= ' TO '. $args{username};

    my $url = $self->_make_url('/query', {
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' && $body eq '{"results":[{}]}' ) {
                $args{on_success}->();
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 revoke_privileges

    $cv = AE::cv;
    $db->revoke_privileges(
        username => 'jdoe',

        # privileges at single database
        database => 'mydb',
        access => 'WRITE',

        # or to revoke cluster administration privileges
        all_privileges => 1,

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to revoke privileges: @_");
        }
    );
    $cv->recv;

Revokes from user C<username> access C<access> on database C<database>.
If flag C<all_privileges> is set it revokes cluster administration privileges
instead.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut


sub revoke_privileges {
    my ($self, %args) = @_;

    my $q = 'REVOKE ';

    if ( $args{all_privileges} ) {
        $q .= 'ALL PRIVILEGES';
    } else {
        $q .= $args{access} .' ON '. $args{database};
    }
    $q .= ' FROM '. $args{username};

    my $url = $self->_make_url('/query', {
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' && $body eq '{"results":[{}]}' ) {
                $args{on_success}->();
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 drop_user

    $cv = AE::cv;
    $db->drop_user(
        username => 'jdoe',

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to drop user: @_");
        }
    );
    $cv->recv;

Drops user C<username>.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut


sub drop_user {
    my ($self, %args) = @_;

    my $q = 'DROP USER '. $args{username};

    my $url = $self->_make_url('/query', {
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' && $body eq '{"results":[{}]}' ) {
                $args{on_success}->();
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head2 Schema Exploration

=head3 show_measurements

    $cv = AE::cv;
    $db->show_measurements(
        database => 'mydb',
        where => "host = 'server02'",

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to list measurements: @_");
        }
    );
    my @measurements = $cv->recv;
    print "$_\n" for @measurements;

Returns names of measurements from database C<database>, filtered by optional
C<where> clause.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub show_measurements {
    my ($self, %args) = @_;

    my $q = 'SHOW MEASUREMENTS';

    if ( my $cond = $args{where} ) {
        $q .= ' WHERE '. $cond;
    }

    my $url = $self->_make_url('/query', {
        db => $args{database},
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' ) {
                my $data = decode_json($body);
                my $res = $data->{results}->[0]->{series}->[0];
                my $values = $res->{values};
                my @measurements = (
                    map { @$_ } @{ $values || [] }
                );
                $args{on_success}->(@measurements);
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 drop_measurement

    $cv = AE::cv;
    $db->drop_measurement(
        database => 'mydb',
        measurement => 'cpu_load',

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to drop measurement: @_");
        }
    );
    $cv->recv;

Drops measurement C<measurement>.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub drop_measurement {
    my ($self, %args) = @_;

    my $q = 'DROP MEASUREMENT '. $args{measurement};

    my $url = $self->_make_url('/query', {
        db => $args{database},
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' && $body eq '{"results":[{}]}' ) {
                $args{on_success}->();
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 show_series

    $cv = AE::cv;
    $db->show_series(
        database => 'mydb',

        measurement => 'cpu_load',
        where => "host = 'server02'",

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to list series: @_");
        }
    );
    my $series = $cv->recv;
    for my $measurement ( sort keys %{ $series } ) {
        print "Measurement: $measurement\n";
        for my $s ( @{ $series->{$measurement} } ) {
            print " * $_: $s->{$_}\n" for sort keys %{ $s };
        }
    }

Returns from database C<database> and optional measurement C<measurement>,
optionally filtered by the C<where> clause, an hash reference with measurements
as keys and their unique tag sets as values.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub show_series {
    my ($self, %args) = @_;

    my $q = 'SHOW SERIES';

    if ( my $measurement = $args{measurement} ) {
        $q .= ' FROM '. $measurement;
    }

    if ( my $cond = $args{where} ) {
        $q .= ' WHERE '. $cond;
    }

    my $url = $self->_make_url('/query', {
        db => $args{database},
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' ) {
                my $data = decode_json($body);
                my $series = {};
                for my $res ( @{ $data->{results}->[0]->{series} || [] } ) {
                    my $cols = $res->{columns};
                    my $values = $res->{values};
                    $series->{ $res->{name } } = [
                        map {
                            +{
                                zip(@$cols, @$_)
                            }
                        } @{ $values || [] }
                    ];
                }
                $args{on_success}->($series);
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 drop_series

    $cv = AE::cv;
    $db->drop_series(
        database => 'mydb',
        measurement => 'cpu_load',
        where => "host = 'server02'",

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to drop measurement: @_");
        }
    );
    $cv->recv;

Drops series from measurement C<measurement> filtered by C<where> clause from
database C<database>.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub drop_series {
    my ($self, %args) = @_;

    my $q = 'DROP SERIES';

    if ( my $measurement = $args{measurement} ) {
        $q .= ' FROM '. $measurement;
    }

    if ( my $cond = $args{where} ) {
        $q .= ' WHERE '. $cond;
    }

    my $url = $self->_make_url('/query', {
        db => $args{database},
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' && $body eq '{"results":[{}]}' ) {
                $args{on_success}->();
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 show_tag_keys

    $cv = AE::cv;
    $db->show_tag_keys(
        database => 'mydb',

        measurement => 'cpu_load',

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to list tag keys: @_");
        }
    );
    my $tag_keys = $cv->recv;
    for my $measurement ( sort keys %{ $tag_keys } ) {
        print "Measurement: $measurement\n";
        print " * $_\n" for @{ $tag_keys->{$measurement} };
    }

Returns from database C<database> and optional measurement C<measurement>,
optionally filtered by the C<where> clause, an hash reference with measurements
as keys and their unique tag keys as values.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub show_tag_keys {
    my ($self, %args) = @_;

    my $q = 'SHOW TAG KEYS';

    if ( my $measurement = $args{measurement} ) {
        $q .= ' FROM '. $measurement;
    }

    if ( my $cond = $args{where} ) {
        $q .= ' WHERE '. $cond;
    }

    my $url = $self->_make_url('/query', {
        db => $args{database},
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' ) {
                my $data = decode_json($body);
                my $tag_keys = {};
                for my $res ( @{ $data->{results}->[0]->{series} || [] } ) {
                    my $values = $res->{values};
                    $tag_keys->{ $res->{name } } = [
                        map {
                            @$_
                        } @{ $values || [] }
                    ];
                }
                $args{on_success}->($tag_keys);
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 show_tag_values

    $cv = AE::cv;
    $db->show_tag_values(
        database => 'mydb',

        measurement => 'cpu_load',

        # single key
        key => 'host',
        # or a list of keys
        keys => [qw( host region )],

        where => "host = 'server02'",

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to list tag values: @_");
        }
    );
    my $tag_values = $cv->recv;
    for my $tag_key ( sort keys %{ $tag_values } ) {
        print "Tag key: $tag_key\n";
        print " * $_\n" for @{ $tag_values->{$tag_key} };
    }

Returns from database C<database> and optional measurement C<measurement>,
values from a single tag key C<key> or a list of tag keys C<keys>, optionally
filtered by the C<where> clause, an hash reference with tag keys
as keys and their unique tag values as values.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub show_tag_values {
    my ($self, %args) = @_;

    my $q = 'SHOW TAG VALUES';

    if ( my $measurement = $args{measurement} ) {
        $q .= ' FROM '. $measurement;
    }

    if ( my $keys = $args{keys} ) {
        $q .= ' WITH KEY IN ('. join(", ", @$keys) .')';
    }
    elsif ( my $key = $args{key} ) {
        $q .= ' WITH KEY = '. $key;
    }

    if ( my $cond = $args{where} ) {
        $q .= ' WHERE '. $cond;
    }

    my $url = $self->_make_url('/query', {
        db => $args{database},
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' ) {
                my $data = decode_json($body);
                my $tag_values = {};
                for my $res ( @{ $data->{results}->[0]->{series} || [] } ) {
                    my $cols = $res->{columns};
                    my $values = $res->{values};
                    $tag_values->{ $cols->[0] } = [
                        map {
                            @$_
                        } @{ $values || [] }
                    ];
                }
                $args{on_success}->($tag_values);
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head2 Continuous Queries

=head3 create_continuous_query

    $cv = AE::cv;
    $db->create_continuous_query(
        database => 'mydb',
        name => 'per5minutes',
        query => 'SELECT MEAN(value) INTO "cpu_load_per5m" FROM cpu_load GROUP BY time(5m)',

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to create continuous query: @_");
        }
    );
    $cv->recv;

Creates new continuous query named by C<name> on database C<database> using
query C<query>.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub create_continuous_query {
    my ($self, %args) = @_;

    my $q = 'CREATE CONTINUOUS QUERY '. $args{name}
        .' ON '. $args{database}
        .' BEGIN '. $args{query}
        .' END';

    my $url = $self->_make_url('/query', {
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' && $body eq '{"results":[{}]}' ) {
                $args{on_success}->();
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 drop_continuous_query

    $cv = AE::cv;
    $db->drop_continuous_query(
        database => 'mydb',
        name => 'per5minutes',

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to drop continuous query: @_");
        }
    );
    $cv->recv;

Drops continuous query named by C<name> on database C<database>.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub drop_continuous_query {
    my ($self, %args) = @_;

    my $q = 'DROP CONTINUOUS QUERY '. $args{name} . ' ON '. $args{database};

    my $url = $self->_make_url('/query', {
        q => $q
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' && $body eq '{"results":[{}]}' ) {
                $args{on_success}->();
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 show_continuous_queries

    $cv = AE::cv;
    $db->show_continuous_queries(
        database => 'mydb',

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to list continuous queries: @_");
        }
    );
    my @continuous_queries = $cv->recv;
    for my $cq ( @continuous_queries ) {
        print "Name: $cq->{name}\n";
        print "Query: $cq->{query}\n";
    }

Returns a list of hash references with keys C<name> and C<query> for each
continuous query defined on database C<database>.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub show_continuous_queries {
    my ($self, %args) = @_;

    my $url = $self->_make_url('/query', {
        db => $args{database},
        q => 'SHOW CONTINUOUS QUERIES'
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' ) {
                my $data = decode_json($body);
                my $res = $data->{results}->[0]->{series}->[0];
                my $cols = $res->{columns};
                my $values = $res->{values};
                my @policies = (
                    map {
                        +{
                            zip(@$cols, @$_)
                        }
                    } @{ $values || [] }
                );
                $args{on_success}->(@policies);
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}


=head2 Writing Data

=head3 write

    $cv = AE::cv;
    $db->write(
        database => 'mydb',
        precision => 'n',
        rp => 'last_day',

        data => [
            # line protocol formatted
            'cpu_load,host=server02,region=eu-east sensor="top",value=0.64 1437868012260500137',

            # or as an hash
            {
                measurement => 'cpu_load',
                tags => {
                    host => 'server02',
                    region => 'eu-east',
                },
                fields => {
                    value => '0.64',
                    sensor => '"top"',
                },
                time => time() * 10**9
            }
        ],

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to write data: @_");
        }
    );
    $cv->recv;

Writes to database C<database> and optional retention policy C<rp>,
time-series data C<data> with optional precision C<precision>. The C<data> can
be specified as single scalar value or as array reference. In either case the
scalar variables are expected to be an formatted using line protocol or if hash
with required keys C<measurement> and C<fields> and optional C<tags> and
C<time>.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub _to_line {
    my $data = shift;

    my $t = $data->{tags} || {};
    my $f = $data->{fields} || {};

    return $data->{measurement}
        .(
            $t ?
                    ','.
                    join(',',
                        map {
                            join('=', $_, $t->{$_})
                        } sort { $a cmp $b } keys %$t
                    )
                :
                ''
        )
        . ' '
        .(
            join(',',
                map {
                    join('=', $_, $f->{$_})
                } keys %$f
            )
        )
        .(
            $data->{time} ?
                ' '. $data->{time}
                :
                ''
        );
}

sub write {
    my ($self, %args) = @_;

    my $data = ref $args{data} eq 'ARRAY' ?
        join("\n", map { ref $_ eq 'HASH' ? _to_line($_) : $_ } @{ $args{data} })
        :
        ref $args{data} eq 'HASH' ? _to_line($args{data}) : $args{data};

    my $url = $self->_make_url('/write', {
        db => $args{database},
        (
            $args{consistency} ?
                ( consistency => $args{consistency} )
                :
                ()
        ),
        (
            $args{rp} ?
                ( rp => $args{rp} )
                :
                ()
        ),
        (
            $args{precision} ?
                ( precision => $args{precision} )
                :
                ()
        )
    });

    $self->_http_request( POST => $url->as_string, $data,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '204' ) {
                $args{on_success}->();
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head2 Querying Data

=head3 select

    $cv = AE::cv;
    $db->select(
        database => 'mydb',
        measurement => 'cpu_load',
        fields => 'host, count(value)',
        where => "region = 'eu-east' AND time > now() - 7d",

        group_by => 'time(5m), host',
        fill => 'previous',

        order_by => 'ASC',

        limit => 10,

        on_success => $cv,
        on_error => sub {
            $cv->croak("Failed to select data: @_");
        }
    );
    my $results = $cv->recv;
    for my $row ( @{ $results } ) {
        print "Measurement: $row->{name}\n";
        print "Tags:\n";
        print " * $_ = $row->{tags}->{$_}\n" for keys %{ $row->{tags} || {} };
        print "Values:\n";
        for my $value ( @{ $row->{values} || [] } ) {
            print " * $_ = $value->{$_}\n" for keys %{ $value || {} };
        }
    }

Executes an select query on database C<database> created from provided arguments
measurement C<measurement>, fields to select C<fields>, optional C<where>
clause, grouped by C<group_by> and empty values filled with C<fill>, ordered by
C<order_by> and number of results limited to C<limit>.

The required C<on_success> code reference is executed if request was successful,
otherwise executes the required C<on_error> code reference.

=cut

sub select {
    my ($self, %args) = @_;

    my $q = 'SELECT '. $args{fields} .' FROM '. $args{measurement};

    if ( my $cond = $args{where} ) {
        $q .= ' WHERE '. $cond;
    }

    if ( my $group = $args{group_by} ) {
        $q .= ' GROUP BY '. $group;

        if ( my $fill = $args{fill} ) {
            $q .= ' fill('. $fill .')';
        }
    }

    if ( my $order_by = $args{order_by} ) {
        $q .= ' ORDER BY '. $order_by;
    }

    if ( my $limit = $args{limit} ) {
        $q .= ' LIMIT '. $limit;

        if ( my $offset = $args{offset} ) {
            $q .= ' OFFSET '. $offset;
        }
    }

    if ( my $slimit = $args{slimit} ) {
        $q .= ' SLIMIT '. $slimit;

        if ( my $soffset = $args{soffset} ) {
            $q .= ' SOFFSET '. $soffset;
        }
    }

    my $url = $self->_make_url('/query', {
        db => $args{database},
        q => $q,
        (
            $args{rp} ?
                ( rp => $args{rp} )
                :
                ()
        ),
    });

    $self->_http_request( GET => $url->as_string,
        sub {
            my ($body, $headers) = @_;

            if ( $headers->{Status} eq '200' ) {
                my $data = decode_json($body);
                my $series = [
                    map {
                        my $res = $_;

                        my $cols = $res->{columns};
                        my $values = $res->{values};

                        +{
                            name => $res->{name},
                            tags => $res->{tags},
                            values => [
                                map {
                                    +{
                                        zip(@$cols, @$_)
                                    }
                                } @{ $values || [] }
                            ]
                        }
                    } @{ $data->{results}->[0]->{series} || [] }
                ];
                $args{on_success}->($series);
            } else {
                $args{on_error}->( $body );
            }
        }
    );
}

=head3 query

    $cv = AE::cv;
    $db->query(
        query => {
            db => 'mydb',
            q => 'SELECT * FROM cpu_load',
        },
        on_response => $cv,
    );
    my ($response_data, $response_headers) = $cv->recv;

Executes an arbitrary query using provided in C<query> arguments.

The required C<on_response> code reference is executed with the raw response
data and headers as parameters.

=cut

sub query {
    my ($self, %args) = @_;

    my $url = $self->_server_uri->clone;
    $url->path('/query');
    $url->query_form_hash( $args{query} );

    $self->_http_request( GET => $url->as_string,
        sub {
            $args{on_response}->(@_);
        }
    );
}

=head1 CAVEATS

Following the optimistic nature of InfluxDB this modules does not validate any
parameters. Also quoting and escaping special characters is to be done by the
user of this library.

=cut

1;
