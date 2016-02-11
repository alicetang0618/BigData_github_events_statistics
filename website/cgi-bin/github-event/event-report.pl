#!/usr/bin/perl -w
# Creates an html table of average daily numbers of events/users by hour of day for the given day of week and event types

use strict;
use warnings;
use 5.10.0;
use HBase::JSONRest;
use CGI qw/:standard/;

my $day = param('day');
my $hour = param('hour');
my @types = param('type');

my $hbase = HBase::JSONRest->new(host => "hadoop-m:2056");

sub cellValue {
    my $row = $_[0];
    my $field_name = $_[1];
    my $row_cells = ${$row}{'columns'};
    foreach my $cell (@$row_cells) {
	if ($$cell{'name'} eq $field_name) {
	    return $$cell{'value'};
	}
    }
    return 'missing';
}

my @records;
foreach my $type (@types){
    my $tmp_records = $hbase->get({
        table => 'github_events_xiaoruit',
        where => {
            key_begins_with => $type.$day.$hour
        },
    });
    @records = (@records, @$tmp_records);
}

my ($user_count, $event_count);
foreach my $row (@records){
    my($user_tmp, $event_tmp) 
      =  (cellValue($row, 'event:user_count'), cellValue($row, 'event:event_count'));
    $user_count += $user_tmp;
    $event_count += $event_tmp;
}
my $date_count = cellValue(@records[0], 'event:date_count');

sub daily_average {
    my($count, $dates) = @_;
    return $dates > 0 ? sprintf("%.1f", $count/$dates) : "-";
}

print header, start_html(-title=>'hello CGI',-head=>Link({-rel=>'stylesheet',-href=>'/xiaoruit/github-event/table.css',-type=>'text/css'}));
print div({-align=>'center'});
print div({-style=>'display:inline-block;box-shadow: 10px 10px 5px #888888;border:1px solid #000000;-moz-border-radius-bottomleft:9px;-webkit-border-bottom-left-radius:9px;border-bottom-left-radius:9px;-moz-border-radius-bottomright:9px;-webkit-border-bottom-right-radius:9px;border-bottom-right-radius:9px;-moz-border-radius-topright:9px;-webkit-border-top-right-radius:9px;border-top-right-radius:9px;-moz-border-radius-topleft:9px;-webkit-border-top-left-radius:9px;border-top-left-radius:9px;background:white'}, '&nbsp;Average Daily Number of Events/Active Users for the Given Day, Hour and Event Type(s) &nbsp;');
print p({-style=>"bottom-margin:10px"});
print "<table class='CSS_Table_Example' style='width:70%;margin:auto;'><tr><td>Number of Events Per Date</td>";
my $size = @types;
if ($size > 1){
    print "<td>Number of Distinct Type-User Pairs Per Date</td></tr><tr>";
}else{
    print "<td>Number of Active Users Per Date</td></tr><tr>";
}
if ($day eq "" or $hour eq ""){
    print "<td>-</td><td>-</td>";
}else{
    print "<td>".daily_average($event_count, $date_count)."</td>";
    print "<td>".daily_average($user_count, $date_count)."</td>";
}
print "</tr></table>";
print p({-style=>"bottom-margin:10px"});
print end_html;
