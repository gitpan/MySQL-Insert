package MySQL::Insert;

use warnings;
use strict;

our $MAX_ROWS_TO_QUERY = 1000;

=head1 NAME

MySQL::Insert - extended inserts for MySQL via DBI

=cut

our $VERSION = '0.02';

=head1 SYNOPSIS

    # Insert two rows into sample_table using $dbh database handle

    use MySQL::Insert;

    $MySQL::Insert::MAX_ROWS_TO_QUERY = 1000;

    my $inserter = MySQL::Insert->new( $dbh, 'sample_table' );

    $inserter->insert_row( { fldname => 'fldvalue1' } );
    $inserter->insert_row( { fldname => 'fldvalue2' } );

    undef $inserter;

=head1 FUNCTIONS / METHODS

The following methods are available:

=head2 new

Create new MySQL::Insert object

=cut

sub new {
    my $type = shift;

    my $self = { };
    $self = bless $self, $type;
    $self->_init( @_ );
    return $self;
}

sub _init {
    my $self = shift;
    my $dbh = shift;
    my $table = shift;
    my $fields = shift;

    $self->{_dbh} = $dbh;
    $self->{_fields} = $fields;
    $self->{_name_fields} = join ", ", @$fields;
    $self->{_table} = $table;
    $self->{_finalize_row} = 1;
    $self->{_query_exists} = 0;
}

DESTROY {
    my $self = shift;

    $self->_finish_row;

    if ($self->{_query_exists}) {
	$self->_execute_query();
    }
}

=head2 insert_row

Schedule row for insertion

=cut

sub insert_row {
    my ($self, $new_row) = @_;

    my $query_executed = $self->_finish_row();

    $self->{_finalize_row} = 0;
    $self->{_row} = $new_row;

    return $query_executed;
}

# Private methods

sub _finish_row {
    my $self = shift;

    my $query_executed;

    if (!$self->{_finalize_row}) {
	if ($self->{_query_exists} && $self->{_total_rows} >= $MAX_ROWS_TO_QUERY) {
	    $query_executed = $self->_execute_query();
	}

	$self->_print_row;

	$self->{_finalize_row} = 1;
    }

    return $query_executed;
}

sub _execute_query {
    my $self = shift;

    my $query = qq|INSERT IGNORE $self->{_table} ($self->{_name_fields}) VALUES |
	. join(", ", @{$self->{_query_rows}}). ";\n";

    my $result = $self->{_dbh}->do( $query ) or return;

    $self->{_query_exists} = 0;
    $self->{_total_rows} = 0;
    @{$self->{_query_rows}} = ();

    return $result;
}

sub _print_row {
    my $self = shift;

    my @data_row = ();
    for my $field (@{$self->{_fields}}) {
	push @data_row, $self->{_dbh}->quote( $self->{_row}->{$field} || '' );
    }

    push @{$self->{_query_rows}}, "\n\t(".join(', ', @data_row).")";

    $self->{_query_exists} = 1;
    $self->{_total_rows}++;
}


=head1 AUTHORS

Gleb Tumanov C<< <gleb at reg.ru> >> (original author)
Walery Studennikov C<< <despair at cpan.org> >> (CPAN distribution)

=head1 BUGS

Please report any bugs or feature requests to C<bug-mysql-insert at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=MySQL-Insert>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 COPYRIGHT & LICENSE

Copyright 2008 Gleb Tumanov (gleb at reg.ru), all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of MySQL::Insert
