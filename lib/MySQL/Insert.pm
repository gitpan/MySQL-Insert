package MySQL::Insert;

use warnings;
use strict;

our $MAX_ROWS_TO_QUERY = 1000;

=head1 NAME

MySQL::Insert - extended inserts for MySQL via DBI

=cut

our $VERSION = '0.01';

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use MySQL::Insert;

    $MySQL::Insert::MAX_ROWS_TO_QUERY = 1000;

    my $inserter = MySQL::Insert->new();

    $inserter->insert_row( { fldname => 'fldvalue' } );

    undef $inserter;

=head1 FUNCTIONS

=head2 new

=cut

sub new {
    my $type = shift;

    my $self = { };
    $self = bless $self, $type;
    $self->_init( @_ );
    return $self;
}

sub table {
    my ($self, $table) = @_;

    if ($table) {
	$self->{_table} = $table;
    }

    return $self->{_table};
}

sub _init {
    my $self = shift;
    my $dbh = shift;
    my $table = shift;
    my $field = shift;

    $self->{_dbh} = $dbh;
    $self->{_fields} = $field;
    $self->{_name_fields} = join ", ", @$field;
    $self->{_table} = $table;
    $self->{_finalize_row} = 1;
    $self->{_query_exists} = 0;
}

DESTROY {
    my $self = shift;

    $self->finish_row;

    if ($self->{_query_exists}) {
	$self->execute_query();
    }
}

=head2 insert_row

=cut

sub insert_row {
    my ($self, $new_row) = @_;

    my $query_executed = $self->finish_row();

    $self->{_finalize_row} = 0;
    $self->{_row} = $new_row;

    return $query_executed;
}

sub finish_row {
    my $self = shift;

    my $query_executed;

    if (!$self->{_finalize_row}) {
	if ($self->{_query_exists} && $self->{_total_rows} >= $MAX_ROWS_TO_QUERY) {
	    $query_executed = $self->execute_query();
	}

	$self->print_row;

	$self->{_finalize_row} = 1;
    }

    return $query_executed;
}

sub execute_query {
    my $self = shift;

    my $query = qq|INSERT IGNORE $self->{_table} ($self->{_name_fields}) VALUES |
	. join(", ", @{$self->{_query_rows}}). ";\n";

    my $result = $self->{_dbh}->do( $query ) or return;

    $self->{_query_exists} = 0;
    $self->{_total_rows} = 0;
    @{$self->{_query_rows}} = ();

    return $result;
}

sub print_row {
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


=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc MySQL::Insert


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=MySQL-Insert>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/MySQL-Insert>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/MySQL-Insert>

=item * Search CPAN

L<http://search.cpan.org/dist/MySQL-Insert>

=back


=head1 ACKNOWLEDGEMENTS


=head1 COPYRIGHT & LICENSE

Copyright 2008 Walery Studennikov, all rights reserved.

This program is free software; you can redistribute it and/or modify it
under the same terms as Perl itself.


=cut

1; # End of MySQL::Insert
